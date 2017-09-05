# DDButil

Helper functions for batch operations in AWS DynamoDB with `DocumentClient` interface:
- run `query`/`scan` operations without worrying about `ExclusiveStartKey`
- run `batchPut`/`batchDelete` (which use `batchWrite` operation internally) without worrying about `UnprocessedItems`
- utilise forwarding functions that call `.promise()` on  docClient function results, so you don't have to

## Installation

`npm install ddbutil`

## Usage

### query example

```js
    const docClient = new aws.DynamoDB.DocumentClient();

    const queryParams = {
        TableName: 'Table_A',
        Select: 'SPECIFIC_ATTRIBUTES',
        ProjectionExpression: 'Attr1, Attr2',
        KeyConditionExpression: 'Attr1 = :Attr1',
        ExpressionAttributeValues: { ':Attr1': '123' },
        ReturnConsumedCapacity: 'TOTAL',
    };

    ddbutil.query(docClient, queryParams)
        .then(items => {
            console.log('Items found:', items.length);
            return items;
        });
```

### batchPut example

```js
    const docClient = new aws.DynamoDB.DocumentClient();

    const writeParams = {
        ReturnConsumedCapacity: 'TOTAL',
    };

    const items = [
        { Attr1: '123', Attr2: 'aaa' },
        { Attr1: '123', Attr2: 'bbb' },
        { Attr1: '123', Attr2: 'ccc' },
    ];

    ddbutil.batchPut(docClient, writeParams, 'Table_B', items)
        .then(putResult => {
            console.log('Put result:', putResult);
            return putResult;
        });
```
