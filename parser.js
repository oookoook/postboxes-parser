const axios = require('axios');
const unzipper = require('unzipper');
const csv = require('fast-csv');
const AWS = require('aws-sdk');
const fs = require('fs');
const iconv = require('iconv-lite');
const gc = require('./sjtsk-converter');
const zipUrl = 'https://www.ceskaposta.cz/documents/10180/3738087/csv_prehled_schranek.zip';

const table = 'postboxes';
AWS.config.update({endpoint: 'https://dynamodb.eu-central-1.amazonaws.com', region: 'eu-central-1'});
const docClient = new AWS.DynamoDB.DocumentClient();

function getExisting() {
    return new Promise(function(resolve, reject) {
        docClient.scan({
            // params
            TableName : table,
            ProjectionExpression:'id, lat, lon, info'
            }
            , function(err, data) {
                if(err) {
                    console.log(err);
                    resolve(false);
                }
                var map = new Map();
                data.Items.forEach(function(item) {
                    map.set(item.id, {
                        lat: item.lat,
                        lon: item.lon,
                        info: item.info,
                        updated: false,
                        changed: false
                    });
                });

                resolve(map);
            });
    })
};

function addNew (id, item) {
    return new Promise(function(resolve, reject) {
        var coords = {lat:0, lon:0};
        if(item.sour_x && item.sour_y) { 
            coords = gc.convert(parseFloat(item.sour_x), parseFloat(item.sour_y), 0);
        } else {
            reject('No coordinates provided');
            return;
        }
        
        // remove the empty strings in the info object
        for(var prop in item) {
            if(!item[prop]) {
                item[prop] = '?';
            }
        }
        
        var params = {
            TableName:table,
            Item:{
                'id': id,
                'lat': coords.lat,
                'lon': coords.lon,
                'info': item
            }};
        docClient.put(params, function(err, data) {
            if (err) {
                reject(err);
            } else {
                resolve(data);
            }
        });

    });
};

function deleteRemoved (existing) {
    var promises = [];
    existing.forEach(function(item, key) {
        if(!item.updated && !item.changed) {
            promises.push(new Promise(function(resolve, reject) {
                docClient.delete({
                    TableName:table,
                    Key:{
                        'id': key,
                    }
                }, function(err, data) {
                    if (err) {
                        resolve({ id: key, err });

                    } else {
                        resolve({ id: key, data});
                    }
                });
            }));
        }
    });
    return Promise.all(promises);
};

function updateExisting(id, updates, attrVals) {
    return new Promise(function(resolve, reject) {
        docClient.update({
            TableName:table,
            Key:{
                'id': id,
            },
            UpdateExpression: 'set ' + updates.join(','),
            ExpressionAttributeValues: attrVals,
            ReturnValues:"UPDATED_NEW"
        },  function(err, data) {
                if (err) {
                    reject(err);
                } else {
                    resolve(data);
                }
            }
        );
    });
};

async function updateDB (useLocal, path) {
    console.log('Starting the job...');
    var errors = 0;
    var inserts = 0;
    var updates = 0;
    var unchanged = 0;

    var existing = await getExisting();
    if(!existing) {
        return;
    }
    console.log(`Loaded ${existing.size} items.`);
    var stream = null;

    // works on AWS lambda
    if(!path) {
        path = '/tmp/postboxes.csv';
    }
    if(!useLocal) {
        console.log('Using remote file...');
        var response = await axios({
            method:'get',
            url:zipUrl,
            responseType:'stream',
        })
        
        // saving the file to temporal location - there is FILE_ENDED error happening when inserting large number of records into the DB and the download stream is 
        // open for a long period of time (probably)
        await new Promise(function(resolve) { response.data.pipe(unzipper.ParseOne()).pipe(fs.createWriteStream(path)).on('finish', () => { console.log('Saving ended.'); resolve() }) });
        stream = fs.createReadStream(path);
    } else {
        console.log('Using local file...')
        stream =  fs.createReadStream(path);
    }

    stream.pipe(iconv.decodeStream('win1250')).pipe(csv(
        {
            headers: true,
            delimiter: ';',

        }
    ))
    .transform(function(data,next){
        // do all the work in the transform so we can run the code synchronously using next callback
        var id = data.psc.toString() + data.cis_schranky.toString();
        var e = existing.get(id);
        if(e) {
            e.updated = true;
            var updates = [];
            var attrVals = {};
            var coords = gc.convert(parseFloat(data.sour_x), parseFloat(data.sour_y), 0);
            if(e.lat != coords.lat) {
                updates.push('lat = :lat');
                attrVals.lat = coords.lat;
            }
            if(e.lon != coords.lon) {
                updates.push('lon = :lon');
                attrVals.lon = coords.lon;
            }
            //[ ['psc','zip'], ['zkrnaz_posty','office'], ['cis_schranky', 'no'],['adresa', 'address'],['sour_x', 'x'],['sour_y','y'],'misto_popis','cast_obce','obec','okres','cas','omezeni'].forEach(function(i) {
            [ 'psc', 'zkrnaz_posty',, 'cis_schranky','adresa','sour_x','sour_y','misto_popis','cast_obce','obec','okres','cas','omezeni'].forEach(function(i) {
                if(data[i] && e.info[i] != data[i]) {
                    var a = ':' + i.replace(/_/g,'');
                    updates.push(`info.${i}=${a}`);
                    attrVals[a] = (data[i]=='')  ? data[i] : '?';
                }
            });
            // update record
            if(updates.length > 0) {
                e.changed = true;
                console.log(`Updating existing item ${id}`);
                updateExisting(id, updates, attrVals)
                    .then(data => { console.log(`Item ${id} updated.`); updates++; next(); })
                    .catch(err => { console.log(`Item ${id} rejected: ${err}`); errors++; next(); })
            } else {
                console.log(`Item ${id} unchanged.`);
                unchanged++;
                next();
            }

        } else {
            console.log(`Adding a new item ${id}: ${JSON.stringify(data)}`);
            addNew(id, data)
                .then(data => { console.log(`Item ${id} added.`); inserts++; next(); })
                .catch(err => { console.log(`Item ${id} rejected: ${err}`); errors++; next(); });
                
        }
    })
    .on("data", function(data){
        // this must be here - without it, the end event won't happen
    })
    .on("end", async function(){
        
        var derr = 0;
        var dcount = 0;
        /*
        var dresults = await deleteRemoved(existing);
        dresults.forEach(i => { if(i.err) { console.log(`Unable to Delete item ${i.id}: ${i.err}`); derr+=1; } else { dcount+=1; console.log(`Item ${i.id} deleted.`); }});
        */
        var output = `Successfull inserts: ${inserts}, Successfull updates: ${updates}, Errors: ${errors}, Successful deletions: ${dcount}, Deletion errors: ${derr}, Unchanged: ${unchanged}`;
        console.log(output);
        console.log('Job finished.');
        return output;
        
    });

} 

module.exports = {
    updateDB
}