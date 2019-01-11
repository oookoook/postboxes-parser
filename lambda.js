const parser = require('./parser');

exports.handler = async function (event, context) {
    return await parser.updateDB();
    //context.succeed('db updated');
};