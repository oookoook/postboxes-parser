const AWS = require('aws-sdk');

const table = 'postboxes';
AWS.config.update({endpoint: 'https://dynamodb.eu-central-1.amazonaws.com', region: 'eu-central-1'});
const docClient = new AWS.DynamoDB.DocumentClient();

function prepareQueryDef(field, val, qd) {
    if(!qd) {
        qd = { updates: [], attrVals: {}};
    }
    var a = ':' + field.replace(/[_.]/g,'');
    qd.updates.push(`${field}=${a}`);
    qd.attrVals[a] = (val)  ? val : '?';
    return qd;
}

function get(id) {
    return new Promise(function(resolve, reject) {
        docClient.get({
            // params
            TableName : table,
            Key:{
                'id': id,
            }
            }
            , function(err, data) {
                if(err) {
                    console.log(err);
                    resolve(false);
                }
                resolve(data.Item);
            });
    })
};

function add (id, coords, item, time) {
    return new Promise(function(resolve, reject) {
        
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
                'updated': time,
                'changed': time,
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

function findRemoved (start) {
    return new Promise(function(resolve, reject) {
        var ids = [];
        var params = {
            TableName : table,
            ProjectionExpression:'id',
            FilterExpression: 'updated < :time',
            ExpressionAttributeValues: {
                ':time': start
            }
        };

        const onScan = function(err, data) {
            if(err) {
                console.log(err);
                resolve(false);
            }
            
            data.Items.forEach(function(item) {
                ids.push(item.id);
            });
            if (typeof data.LastEvaluatedKey != 'undefined') {
                params.ExclusiveStartKey = data.LastEvaluatedKey;
                docClient.scan(params, onScan);
            } else {
                resolve(ids);
            }
        }
        docClient.scan(params, onScan);
    });
}

function deleteIds (ids) {
    var promises = [];
    ids.forEach(id => {
        promises.push(new Promise(function(resolve, reject) {
            docClient.delete({
                TableName:table,
                Key:{
                    'id': id,
                }
            }, function(err, data) {
                if (err) {
                    resolve({ id, err });
                } else {
                    resolve({ id, data});
                }
            });
        }));
    });
    return Promise.all(promises);
};

function update(id, qd) {
    return new Promise(function(resolve, reject) {
        docClient.update({
            TableName:table,
            Key:{
                'id': id,
            },
            UpdateExpression: 'set ' + qd.updates.join(','),
            ExpressionAttributeValues: qd.attrVals,
            ReturnValues:'UPDATED_NEW'
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

module.exports = {
    prepareQueryDef,
    get,
    add,
    findRemoved,
    deleteIds,
    update   
}