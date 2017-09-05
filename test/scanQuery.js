'use strict';

const sinon = require('sinon');
const { expect } = require('chai').use(require('sinon-chai'));

const ddbutil = require('../lib');

const resultSets = [
    [
        { f1: 11, f2: 21 },
        { f1: 12, f2: 22 },
        { f1: 13, f2: 23 },
    ],
    [
        { f1: 14, f2: 24 },
        { f1: 15, f2: 25 },
    ],
];

const resultSetsAllItems = resultSets.reduce((acc, rs) => acc.concat(rs), []);

const stubQueryOrScan = () => sinon.stub()
    .onCall(0).returns({ promise: () => Promise.resolve({ Items: resultSets[0], LastEvaluatedKey: 13 }) })
    .onCall(1).returns({ promise: () => Promise.resolve({ Items: resultSets[1] }) });


['scan', 'query'].forEach(func => {

    describe(func, function () {

        it(`calls docClient.${func}() with correct arguments and returns items`, function () {
            const docClient = { [func]: stubQueryOrScan() };

            return Promise.resolve()
                .then(() => ddbutil[func](docClient, { TableName: 'table1' }))
                .then(allItems => {
                    expect(allItems).to.be.eql(resultSetsAllItems);

                    expect(docClient[func]).calledTwice;
                    expect(docClient[func]).calledWith({ TableName: 'table1', ExclusiveStartKey: undefined });
                    expect(docClient[func]).calledWith({ TableName: 'table1', ExclusiveStartKey: 13 });
                });
        });

        it(`calls progressCallback for every docClient.${func}() call`, function () {

            const docClient = { [func]: stubQueryOrScan() };

            const scanSummary = {};
            const scanResult = { Items: [] };

            const progressCallback = (result, summary) => {
                scanResult.Items = scanResult.Items.concat(result.Items);
                Object.assign(scanSummary, summary);
            };

            return Promise.resolve()
                .then(() => ddbutil[func](docClient, { TableName: 'table1' }, progressCallback))
                .then(allItems => {
                    expect(scanResult.Items).to.be.eql(allItems);

                    expect(scanSummary.OutItemCount).to.be.equal(resultSetsAllItems.length);
                    expect(scanSummary.ApiCallCount).to.be.equal(2);
                });
        });

    });

});

