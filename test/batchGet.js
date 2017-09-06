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


const stubBatchGet = () => sinon.stub()
    .onCall(0).returns({ promise: () => Promise.resolve({
        Responses: { table1: inputSet.slice(0, 2) },
        UnprocessedKeys: ddbutil.buildGetParams('table1', inputSet.slice(2)) })
    })
    .onCall(1).returns({ promise: () => Promise.resolve({
        Responses: { table1: inputSet.slice(2) },
        UnprocessedKeys: {} })
    });


describe('batchGet', function () {

    it(`calls docClient.batchWrite() with correct arguments`, function () {
        const docClient = { batchGet: stubBatchGet() };

        return Promise.resolve()
            .then(() => ddbutil.batchGet(docClient, {}, 'table1', inputSet))
            .then(result => {
                expect(docClient.batchGet).calledWith({ RequestItems: ddbutil.buildGetParams('table1', inputSet) });
                expect(docClient.batchGet).calledWith({ RequestItems: ddbutil.buildGetParams('table1', inputSet.slice(2)) });
                expect(docClient.batchGet).calledTwice;
            });
    });


    it(`calls progressCallback for every docClient.batchWrite() call and returns expected items`, function () {
        const docClient = { batchGet: stubBatchGet() };

        const scanSummary = {};
        const progressCallback = (result, summary) => {
            Object.assign(scanSummary, summary);
        };

        return Promise.resolve()
            .then(() => ddbutil.batchGet(docClient, {}, 'table1', inputSet, progressCallback))
            .then(result => {
                expect(result).eql(inputSet);
                expect(scanSummary.ApiCallCount).to.be.equal(2);
            });
    });

});
