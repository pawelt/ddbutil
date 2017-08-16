'use strict';


/**
 * Use DocumentClient.query() to fetch items in one go.
 *
 * @param {DocumentClient} docClient DocumentClient instance
 * @param {Object} params an object with parameters for DocumentClient.query()
 * @param {Function} [progressCallback] a function called after every DocumentClient.query() call
 * @returns {Promise.<Array>} array of items
 */
function query(docClient, params, progressCallback) {
    return ddbQueryOrScan(docClient, params, 'query', progressCallback);
}


/**
 * Use DocumentClient.scan() to fetch items in one go.
 *
 * @param {DocumentClient} docClient DocumentClient instance
 * @param {Object} params an object with parameters for DocumentClient.scan()
 * @param {Function} [progressCallback] a function called after every DocumentClient.scan() call
 * @returns {Promise.<Array>} array of items
 */
function scan(docClient, params, progressCallback) {
    return ddbQueryOrScan(docClient, params, 'scan', progressCallback);
}


/**
 * Use DocumentClient.batchWrite() to PUT items into a table in one go.
 *
 * @param {DocumentClient} docClient DocumentClient instance
 * @param {Object} params an object with optional keys: ReturnConsumedCapacity and ReturnItemCollectionMetrics
 * @param {String} tableName
 * @param {Array} items an array of objects to put
 * @param {Function} [progressCallback] a function called after every DocumentClient.batchWrite() call
 * @returns {Promise.<Object>} { ItemCount: 0, ConsumedCapacity: 0 }
 */
function batchPut(docClient, params, tableName, items, progressCallback) {
    const buildPutParams = (tableName, items) => ({ [tableName]: items.map(Item => ({ PutRequest: { Item } })) });
    return ddbBatchWrite(docClient, params, tableName, items, buildPutParams, progressCallback);
}


/**
 * Use DocumentClient.batchWrite() to DELETE items from a table in one go.
 *
 * @param {DocumentClient} docClient DocumentClient instance
 * @param {Object} params an object with optional keys: ReturnConsumedCapacity and ReturnItemCollectionMetrics
 * @param {String} tableName
 * @param {Array} items an array of objects to delete
 * @param {Function} [progressCallback] a function called after every DocumentClient.batchWrite() call
 * @returns {Promise.<Object>} { ItemCount: 0, ConsumedCapacity: 0 }
 */
function batchDelete(docClient, params, tableName, items, progressCallback) {
    const buildDeleteParams = (tableName, items) => ({ [tableName]: items.map(Key => ({ DeleteRequest: { Key } })) });
    return ddbBatchWrite(docClient, params, tableName, items, buildDeleteParams, progressCallback);
}


function ddbQueryOrScan(docClient, params, method, progressCallback) {

    const summary = {
        TableName: params.TableName,
        ConsumedCapacity: 0,
        OutItemCount: 0,
    };

    let items = [];
    const fetchItems = ExclusiveStartKey => {
        return docClient[method](Object.assign(params, { ExclusiveStartKey })).promise()
            .then(result => {
                items = items.concat(result.Items);
                summary.OutItemCount += result.Items.length;
                summary.ConsumedCapacity += (result.ConsumedCapacity || {}).CapacityUnits || 0;
                progressCallback && progressCallback(result, summary);

                return result.LastEvaluatedKey
                    ? fetchItems(result.LastEvaluatedKey)
                    : items;
            });
    };

    return fetchItems();
}


function ddbBatchWrite(docClient, params, tableName, items, buildParams, progressCallback) {

    const summary = {
        TableName: tableName,
        ConsumedCapacity: 0,
        InItemCount: items.length,
    };

    const processChunks = chunks => {
        const writeChunk = (p, chunk) => p
            .then(_ => buildParams(tableName, chunk))
            .then(writeItems);
        return chunks.reduce(writeChunk, Promise.resolve());
    };

    const writeItems = RequestItems => {
        return docClient.batchWrite(Object.assign(params, { RequestItems })).promise()
            .then(result => {
                (result.ConsumedCapacity || []).forEach(cu => summary.ConsumedCapacity += cu.CapacityUnits);
                progressCallback && progressCallback(result, summary);

                return Object.keys(result.UnprocessedItems).length
                    ? writeItems(result.UnprocessedItems)
                    : RequestItems;
            });
    };

    return Promise.resolve(chunk(items, 25))
        .then(processChunks)
        .then(_ => summary);
}


function chunk(array, chunkSize) {
    if (!array || array.length === 0) return [];
    if (!chunkSize) return [array];
    const chunkCount = Math.ceil(array.length / chunkSize);
    const result = Array.apply(null, { length: chunkCount });
    for (let c = 0, i = 0; c < chunkCount; c++)
        result[c] = array.slice(i, (i += chunkSize));
    return result;
}


module.exports = {
    query,
    scan,
    batchPut,
    batchDelete,
};
