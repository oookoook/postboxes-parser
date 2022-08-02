/*
This does not scale well (or, at all) on AWS lambda since there is a 15 minutes timeout and the batch can take a longer time.
But we are able to process the input file for Czech Republic in one go, so let's keep it this way now.

Since this is a batch work that is executed once a month, it would probably be reasonable to star an EC2 instance, process the data
and shut it down afterwards... 
*/

const axios = require('axios');
const unzipper = require('unzipper');
const cwait = require('cwait');
const csv = require('fast-csv');
const fs = require('fs');
const iconv = require('iconv-lite');
const gc = require('./sjtsk-converter');
const db = require('./db-service');
const zipUrl = 'https://www.ceskaposta.cz/documents/10180/3738087/csv_prehled_schranek.zip';

const queue = new cwait.TaskQueue(Promise, 100);

function updateItem(data, progress) {
    return new Promise(function (resolve, reject) {
        var id = data.psc.toString() + data.cis_schranky.toString();
        progress.total+=1;
        if(progress.total % 1000 == 0) {
            console.log(`Processed ${progress.total} items.`);
        }
        db.get(id).then(e => {
            //console.dir(e);
            if(e) {
            var qd = null;
            if(!(data.sour_x && data.sour_y)) {
                // if coords were removed do nothing - this deletes the record as the updated atribute is not refreshed
                console.log(`Existing item ${id} has corrds removed - marked for deletion`);
                resolve();
                return;
            }
            var coords = gc.convert(parseFloat(data.sour_x), parseFloat(data.sour_y), 0);
            if(e.lat != coords.lat) {
                qd = db.prepareQueryDef('lat', coords.lat, qd);
            }
            if(e.lon != coords.lon) {
                qd = db.prepareQueryDef('lon', coords.lon, qd);
            }
            //[ ['psc','zip'], ['zkrnaz_posty','office'], ['cis_schranky', 'no'],['adresa', 'address'],['sour_x', 'x'],['sour_y','y'],'misto_popis','cast_obce','obec','okres','cas','omezeni'].forEach(function(i) {
            try {
            [ 'psc', 'zkrnaz_posty', 'cis_schranky','adresa','sour_x','sour_y','misto_popis','cast_obce','obec','okres' ].forEach(function(i) {
                if(data[i] && (!e.info || e.info[i] != data[i])) {
                   console.log(`Difference: ${id} ${i} ${e.info[i]} ${data[i]}`)
                  qd = db.prepareQueryDef(`info.${i}`, data[i], qd);
                }
            });
            // TODO remove also invalid old values (this code only add new ones)
            [ 'cas','omezeni' ].forEach(function(i) {
                if(data[i] && (!e.info || e.info[i].indexOf(data[i]) == -1)) {
                    console.log(`Difference: ${id} ${i} ${e.info[i]} ${data[i]}`)
                    qd = db.prepareQueryDef(`info.${i}`, e.info[i]+';'+data[i], qd);
                }
            });
            } catch(ex) {
                console.log(ex);
                console.dir(e);
                console.dir(data);
                resolve();
                return;
            }
            // update record
            var u = true;
            
            if(qd) {
                qd = db.prepareQueryDef('changed', progress.start, qd);
                console.log(`Updating existing item ${id}`);
                
            } else {
                u=false;
                //console.log(`Item ${id} unchanged.`);
            }
            qd = db.prepareQueryDef('updated', progress.start, qd);
            db.update(id, qd)
                    .then(data => { /*console.log(`Item ${id} updated.`);*/ if (u) progress.updates++; else progress.unchanged++; resolve(); })
                    .catch(err => { console.log(`Item ${id} rejected: ${err}`); progress.errors++; resolve(); })

        } else {
            var coords = {lat:0, lon:0};
            if(data.sour_x && data.sour_y) { 
                coords = gc.convert(parseFloat(data.sour_x), parseFloat(data.sour_y), 0);
            } else {
                //console.log(`Item ${id} ignored - no coords provided.`);
                progress.ignored++;
                resolve();
                return;
            }
            console.log(`Adding a new item ${id}: ${JSON.stringify(data)}`);
            db.add(id, coords, data, progress.start)
                .then(data => { console.log(`Item ${id} added.`); progress.inserts++; resolve(); })
                .catch(err => { console.log(`Item ${id} rejected: ${err}`); progress.errors++; resolve(); });
                
        }
        });
    }); 
}

var qui = queue.wrap(updateItem);

async function updateDB (useLocal, path) {
    console.log('Starting the job...');
    var progress = {
        total: 0,
        errors: 0,
        inserts: 0,
        updates: 0,
        unchanged: 0,
        ignored: 0,
        deletions: {
            errors: 0,
            count: 0
        },
        start: new Date().valueOf(),
    }
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

    var checks = [];

    await new Promise(function(resolve) {
    stream.pipe(iconv.decodeStream('win1250')).pipe(csv(
        {
            headers: true,
            delimiter: ';',
            quote: null,
            discardUnmappedColumns: true

        }
    ))
    .on("data", function(data){
        checks.push(qui(data, progress));
    })
    .on("error", function(data) {
        console.log('handling error', x, y, data);
        return false;
    })
    .on("end", async function(){
        // this resolves when all the records are processed from the csv and added to the checks array
        // the await is way above where the stream processing begins 
        resolve();  
    });
    });

    // now wait until all the checks are completed
    await Promise.all(checks);
    
    var idsToDelete = await db.findRemoved(progress.start);
    console.log(`${idsToDelete.length} items marked to delete....`);    
    var dresults = await db.deleteIds(idsToDelete);
    dresults.forEach(i => { if(i.err) { console.log(`Unable to Delete item ${i.id}: ${i.err}`); progress.deletions.errors+=1; } else { progress.deletions.count+=1; console.log(`Item ${i.id} deleted.`); }});
    var output = `Successfull inserts: ${progress.inserts}, Successfull updates: ${progress.updates}, Errors: ${progress.errors}, Successful deletions: ${progress.deletions.count}, Deletion errors: ${progress.deletions.errors}, Unchanged: ${progress.unchanged}, Ignored: ${progress.ignored}`;
    console.log(output);
    console.log(`Job finished in ${((new Date()).valueOf() - progress.start) / 1000} s.`);
    return output;
} 

module.exports = {
    updateDB,
}