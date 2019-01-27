/*
USAGE:

node local.js download ./download.csv
OR
node local.js local ./test.csv

*/

const parser = require('./parser');

(async () => {
    var download = true;
    var path = null;
    if(process.argv[2] && process.argv[2] == 'local') {
        download = false;
    }
    if(process.argv[3]) {
        path = process.argv[3]; 
    }

    await parser.updateDB(!download, path);
})();
