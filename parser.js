const axios = require('axios');
const unzipper = require('unzipper');
const csv = require('fast-csv');
const fs = require('fs');
const iconv = require('iconv-lite');
const gc = require('./sjtsk-converter');
const db = require('./db-service');
const zipUrl = 'https://www.ceskaposta.cz/documents/10180/3738087/csv_prehled_schranek.zip';

async function updateDB (useLocal, path) {
    console.log('Starting the job...');
    var total = 0;
    var errors = 0;
    var inserts = 0;
    var updates = 0;
    var unchanged = 0;
    var start = new Date().valueOf();
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

    return await new Promise(function(resolve) {
    stream.pipe(iconv.decodeStream('win1250')).pipe(csv(
        {
            headers: true,
            delimiter: ';',

        }
    ))
    .transform(function(data,next){
        // do all the work in the transform so we can run the code synchronously using next callback
        var id = data.psc.toString() + data.cis_schranky.toString();
        total+=1;
        if(total % 1000 == 0) {
            console.log(`Processed ${total} items.`);
        }
        db.get(id).then(e => {
            //console.dir(e);
            if(e) {
            var qd = null;
            var coords = gc.convert(parseFloat(data.sour_x), parseFloat(data.sour_y), 0);
            if(e.lat != coords.lat) {
                qd = db.prepareQueryDef('lat', coords.lat, qd);
            }
            if(e.lon != coords.lon) {
                qd = db.prepareQueryDef('lon', coords.lon, qd);
            }
            //[ ['psc','zip'], ['zkrnaz_posty','office'], ['cis_schranky', 'no'],['adresa', 'address'],['sour_x', 'x'],['sour_y','y'],'misto_popis','cast_obce','obec','okres','cas','omezeni'].forEach(function(i) {
            [ 'psc', 'zkrnaz_posty',, 'cis_schranky','adresa','sour_x','sour_y','misto_popis','cast_obce','obec','okres','cas','omezeni'].forEach(function(i) {
                if(data[i] && e.info[i] != data[i]) {
                   console.log(`Difference: ${id} ${i} ${e.info[i]} ${data[i]}`)
                  qd = db.prepareQueryDef(`info.${i}`, data[i], qd);
                }
            });
            // update record
            var u = true;
            if(qd) {
                qd = db.prepareQueryDef('changed', start, qd);
                console.log(`Updating existing item ${id}`);
                
            } else {
                u=false;
                //console.log(`Item ${id} unchanged.`);
            }
            qd = db.prepareQueryDef('updated', start, qd);
            db.update(id, qd)
                    .then(data => { /*console.log(`Item ${id} updated.`);*/ if (u) updates++; else unchanged++; next(); })
                    .catch(err => { console.log(`Item ${id} rejected: ${err}`); errors++; next(); })

        } else {
            console.log(`Adding a new item ${id}: ${JSON.stringify(data)}`);
            db.add(id, start, data)
                .then(data => { console.log(`Item ${id} added.`); inserts++; next(); })
                .catch(err => { console.log(`Item ${id} rejected: ${err}`); errors++; next(); });
                
        }
        });
    })
    .on("data", function(data){
        // this must be here - without it, the end event won't happen
    })
    .on("end", async function(){
        
        var derr = 0;
        var dcount = 0;
        
        var dresults = await db.deleteIds(await db.findRemoved(start));
        dresults.forEach(i => { if(i.err) { console.log(`Unable to Delete item ${i.id}: ${i.err}`); derr+=1; } else { dcount+=1; console.log(`Item ${i.id} deleted.`); }});
        var output = `Successfull inserts: ${inserts}, Successfull updates: ${updates}, Errors: ${errors}, Successful deletions: ${dcount}, Deletion errors: ${derr}, Unchanged: ${unchanged}`;
        console.log(output);
        console.log('Job finished.');
        //return output;
        resolve(output);  
    });
    });
} 

module.exports = {
    updateDB
}