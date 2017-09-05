'use strict';

const sinon = require('sinon');
const { expect } = require('chai').use(require('sinon-chai'));

const ddbutil = require('../lib');

const inputSet = [
    { f1: 11, f2: 21 },
    { f1: 12, f2: 22 },
    { f1: 13, f2: 23 },
    { f1: 14, f2: 24 },
    { f1: 15, f2: 25 },
];


const stubBatchWrite = (buildItems) => sinon.stub()
    .onCall(0).returns({ promise: () => Promise.resolve({ UnprocessedItems: buildItems('table1', inputSet.slice(2)) }) })
    .onCall(1).returns({ promise: () => Promise.resolve({ UnprocessedItems: {} }) });


const paramMappers = {
    batchPut: ddbutil.buildPutParams,
    batchDelete: ddbutil.buildDeleteParams,
};


Object.keys(paramMappers).forEach(func => {

    describe(func, function () {

        it(`calls docClient.batchWrite() with correct arguments`, function () {
            const docClient = { batchWrite: stubBatchWrite(paramMappers[func]) };

            return Promise.resolve()
                .then(() => ddbutil[func](docClient, {}, 'table1', inputSet))
                .then(result => {
                    expect(docClient.batchWrite).calledWith({ RequestItems: paramMappers[func]('table1', inputSet) });
                    expect(docClient.batchWrite).calledWith({ RequestItems: paramMappers[func]('table1', inputSet.slice(2)) });
                    expect(docClient.batchWrite).calledTwice;
                });
        });


        it(`calls progressCallback for every docClient.batchWrite() call`, function () {
            const docClient = { batchWrite: stubBatchWrite(paramMappers[func]) };

            const scanSummary = {};
            const progressCallback = (result, summary) => {
                Object.assign(scanSummary, summary);
            };

            return Promise.resolve()
                .then(() => ddbutil[func](docClient, {}, 'table1', inputSet, progressCallback))
                .then(result => {
                    expect(result.InItemCount).equal(inputSet.length);

                    expect(scanSummary.ApiCallCount).to.be.equal(2);
                });
        });

    });

});


