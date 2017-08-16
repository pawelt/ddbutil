'use strict';

const { chunk, forEach } = require('lodash');


/**
 * Use DocumentClient.query() to fetch items in one go.
 *
 * @param {DocumentClient} docClient DocumentClient instance
 * @param {Object} params an object with parameters for DocumentClient.query()
 * @returns {Promise.<Array>} array of items
 */
function query(docClient, params) {
    return ddbQueryOrScan(docClient, params, [], void 0, 'query');
}


/**
 * Use DocumentClient.scan() to fetch items in one go.
 *
 * @param {DocumentClient} docClient DocumentClient instance
 * @param {Object} params an object with parameters for DocumentClient.scan()
 * @returns {Promise.<Array>} array of items
 */
function scan(docClient, params) {
    return ddbQueryOrScan(docClient, params, [], void 0, 'scan');
}


/**
 * Use DocumentClient.batchWrite() to PUT items into a table in one go.
 *
 * @param {DocumentClient} docClient DocumentClient instance
 * @param {Object} params an object with optional keys: ReturnConsumedCapacity and ReturnItemCollectionMetrics
 * @param {String} tableName
 * @param {Array} items an array of objects to put
 * @returns {Promise.<Object>} { ItemCount: 0, ConsumedCapacity: 0 }
 */
function batchPut(docClient, params, tableName, items) {
    const buildPutParams = (tableName, items) => ({ [tableName]: items.map(Item => ({ PutRequest: { Item } })) });
    return ddbBatchWrite(docClient, params, tableName, items, buildPutParams);
}


/**
 * Use DocumentClient.batchWrite() to DELETE items from a table in one go.
 *
 * @param {DocumentClient} docClient DocumentClient instance
 * @param {Object} params an object with optional keys: ReturnConsumedCapacity and ReturnItemCollectionMetrics
 * @param {String} tableName
 * @param {Array} items an array of objects to delete
 * @returns {Promise.<Object>} { ItemCount: 0, ConsumedCapacity: 0 }
 */
function batchDelete(docClient, params, tableName, items) {
    const buildDeleteParams = (tableName, items) => ({ [tableName]: items.map(Key => ({ DeleteRequest: { Key } })) });
    return ddbBatchWrite(docClient, params, tableName, items, buildDeleteParams);
}



function ddbQueryOrScan(docClient, params, items = [], LastEvaluatedKey, method) {

    params.ExclusiveStartKey = LastEvaluatedKey;

    return docClient[method](params).promise().then(({Items, LastEvaluatedKey}) => {
        const newItems = items.concat(Items);

        return LastEvaluatedKey && newItems.length < 1000
            ? ddbQueryOrScan(docClient, params, newItems, LastEvaluatedKey, method)
            : newItems;
    });
}


function ddbBatchWrite(docClient, params, tableName, items, buildParams) {

    const result = {
        ItemCount: items.length,
        ConsumedCapacity: 0,
    };

    const processChunks = chunks => {
        return chunks.reduce((promisedResult, chunk) => {
            return promisedResult
                .then(_ => buildParams(tableName, chunk))
                .then(writeItems)
        }, Promise.resolve());
    };

    const writeItems = RequestItems =>
        docClient.batchWrite(Object.assign(params, { RequestItems })).promise()
            .then(({ UnprocessedItems, ConsumedCapacity }) => {
                forEach(ConsumedCapacity, x => result.ConsumedCapacity += x.CapacityUnits);
                return Object.keys(UnprocessedItems).length
                    ? writeItems(UnprocessedItems)
                    : RequestItems;
            });

    return Promise.resolve(chunk(items, 25))
        .then(processChunks)
        .then(_ => result);
}


module.exports = {
    query,
    scan,
    batchPut,
    batchDelete,
};
