'use strict';

const {
          query,
          scan,

          batchGet,
          batchPut,
          batchDelete,

          buildGetParams,
          buildPutParams,
          buildDeleteParams
      } = require('./utils');

module.exports = {

    query,
    scan,

    batchGet,
    batchPut,
    batchDelete,

    buildGetParams,
    buildPutParams,
    buildDeleteParams,

    get: (docClient, params) => docClient.get(params).promise(),
    put: (docClient, params) => docClient.put(params).promise(),
    update: (docClient, params) => docClient.update(params).promise(),
    delete: (docClient, params) => docClient.delete(params).promise(),

    original: {
        query: (docClient, params) => docClient.query(params).promise(),
        scan: (docClient, params) => docClient.scan(params).promise(),
        batchGet: (docClient, params) => docClient.batchGet(params).promise(),
        batchPut: (docClient, params) => docClient.batchPut(params).promise(),
        batchDelete: (docClient, params) => docClient.batchDelete(params).promise(),
    },
};
