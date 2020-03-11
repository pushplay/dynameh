# Dynameh
DynamoDB on Node more easier

Dynameh makes the official [DynamoDB JavaScript API](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html) easier to use.  It makes it easier to:

- [build request objects](https://giftbit.github.io/dynameh/modules/_requestbuilder_.html)
- [unwrap response objects](https://giftbit.github.io/dynameh/modules/_responseunwrapper_.html)
- [build a projection expression](https://giftbit.github.io/dynameh/modules/_requestbuilder_.html#addprojection)
- [build a condition expression](https://giftbit.github.io/dynameh/modules/_requestbuilder_.html#addcondition)
- [run oversized batch requests](https://giftbit.github.io/dynameh/modules/_batchhelper_.html)
- [paginate through queries](https://giftbit.github.io/dynameh/modules/_queryhelper_.html)
- [paginate through scans](https://giftbit.github.io/dynameh/modules/_scanhelper_.html)
- [run requests concurrently that can't be run in batch](https://giftbit.github.io/dynameh/modules/_concurrenthelper_.html)
- [implement optimistic locking](https://giftbit.github.io/dynameh/interfaces/_tableschema_.tableschema.html#versionkeyfield)
- [configure Date serialization](https://giftbit.github.io/dynameh/interfaces/_tableschema_.tableschema.html#dateserializationfunction)

## Installation

Dynameh is your typical NPM package.

```bash
npm install --save dynameh
```

## Usage

```javascript
import * as dynameh from "dynameh";
// or
const dynameh = require("dynameh");
```

See the [documentation](https://giftbit.github.io/dynameh/) for details on each module and its methods.

### A Simple Example

This example is written using async/await which is available in TypeScript and ES2017.

```javascript
import * as aws from "aws-sdk";
import * as dynameh from "dynameh";

// Initialize the DynamoDB client.
const dynamodb = new aws.DynamoDB({
    apiVersion: "2012-08-10",
    region: "us-west-1"
});

// Set up the table schema.
const tableSchema = {
    tableName: "motorcycles",
    partitionKeyField: "id",
    partitionKeyType: "string"
};

async function updateMotorcycleHorsePower(motorcycleId, bhp) {
    // Fetch the item from the database.
    const getRequest = dynameh.requestBuilder.buildGetInput(tableSchema, motorcycleId);
    const getResult = await dynamodb.getItem(getRequest).promise();
    let motorcycle = dynameh.responseUnwrapper.unwrapGetOutput(getResult);
    
    if (!motorcycle) {
        // Item not found, create it.
        motorcycle = {
            id: motorcycleId
        };
    }
    
    // Update the horse power stat.
    motorcycle.bhp = bhp;
    
    // Put the updated object in the database.
    const putRequest = dynameh.requestBuilder.buildPutInput(tableSchema, motorcycle);
    await dynamodb.putItem(putRequest).promise();
}

updateMotorcycleHorsePower("sv-650", 73.4);
```

### TableSchema

The key to easy building of requests in Dynameh is the [TableSchema](https://giftbit.github.io/dynameh/interfaces/_tableschema_.tableschema.html).  This simple object defines all the extra information Dynameh needs to build requests.

For a table called `MyTable` with a partition key `id` that is a number...

```json
{
  "tableName": "MyTable",
  "partitionKeyField": "id",
  "partitionKeyType": "number"
}
```

For a table called `MyAdvancedTable` with a partition key `id` that is a string, a sort key `date` that is a number, and a version field `version`...

```json
{
  "tableName": "MyAdvancedTable",
  "partitionKeyField": "id",
  "partitionKeyType": "string",
  "sortKeyField": "date",
  "sortKeyType": "number",
  "versionKeyField": "version"
}
```

### Optimistic Locking

Optimistic locking is a strategy for preventing changes from clobbering each other.  For example two processes read from the database, make unrelated changes, and then both write to the database but the second write overwrites the first (clobbers).

Enable optimistic locking by setting the `versionKeyField` on your TableSchema.  In the second TableSchema example that field is `version`.  The `versionKeyField` will be automatically incremented on the server side during a put request.  If the value for `versionKeyField` sent does not match the current value in the database then the contents have changed since the last get and the optimistic lock has failed.  In that case you should get the latest version from the database and replay the update against that.

```javascript
import * as aws from "aws-sdk";
import * as dynameh from "dynameh";

// Initialize the DynamoDB client.
const dynamodb = new aws.DynamoDB({
    apiVersion: "2012-08-10",
    region: "us-west-1"
});

// Set up the table schema.
const tableSchema = {
    tableName: "motorcycles",
    partitionKeyField: "id",
    partitionKeyType: "string",
    versionKeyField: "version"
};

async function updateMotorcycleHorsePower(motorcycleId, bhp) {
    // Fetch the item from the database.
    const getRequest = dynameh.requestBuilder.buildGetInput(tableSchema, motorcycleId);
    const getResult = await dynamodb.getItem(getRequest).promise();
    let motorcycle = dynameh.responseUnwrapper.unwrapGetOutput(getResult);
    
    if (!motorcycle) {
        // Item not found, create it.
        // Note that we don't need to set the version on create.
        motorcycle = {
            id: motorcycleId
        };
    }
    
    // Update the horse power stat.
    motorcycle.bhp = bhp;
    
    // Put the updated object in the database.
    const putRequest = dynameh.requestBuilder.buildPutInput(tableSchema, motorcycle);
    try {
        await dynamodb.putItem(putRequest).promise();
    } catch (err) {
        if (err.code === "ConditionalCheckFailedException") {
            // If this is the error code then the optimistic locking has failed
            // and we should redo the update operation (done here with recursion).
            updateMotorcycleHorsePower(motorcycleId, bhp);
        } else {
            throw err;
        }
    }
}

updateMotorcycleHorsePower("sv-650", 73.4);
```

### Query and Scan

Both query and scan are paginated operations.  They may return a limited set of results and you must make repeated calls to get all the results.  [queryHelper](https://giftbit.github.io/dynameh/modules/_queryhelper_.html) and [scanHelper](https://giftbit.github.io/dynameh/modules/_scanhelper_.html) provide utilities that go through the pagination for you.

`queryAll()` and `scanAll()` will collect all results in a single array.  For small result sets this is easiest to work with but for large result sets this may require a lot of memory to store all results at once.

`queryByCallback()` and `scanByCallback()` take a callback parameter that processes each page of results.  Processing one page at a time reduces the amount of memory required.

This example function scans the table for items and deletes them in batches:

```javascript
async function deleteAllItems(dynamodbClient, tableSchema) {
    const scanInput = dynameh.requestBuilder.buildScanInput(tableSchema);
    let deleteCount = 0;
    await dynameh.scanHelper.scanByCallback(dynamodbClient, scanInput, async items => {
        const keysToDelete = objectSchema.sortKeyField ?
            items.map(item => [item[tableSchema.partitionKeyField], item[tableSchema.sortKeyField]]) :
            items.map(item => item[tableSchema.partitionKeyField]);

        const batchDeleteInput = dynameh.requestBuilder.buildBatchDeleteInput(tableSchema, keysToDelete);
        await dynameh.batchHelper.batchWriteAll(dynamodbClient, batchDeleteInput);
        console.log("deleted", (deleteCount += keysToDelete.length), "items");
        return true;
    });
}
```

### Conditions

Conditions can be added to a put or delete request to make the operation conditional.

One of the most useful conditions is that the item must not already exist (create but not update).  This is done by asserting `attribute_not_exists` on the primary key.  For example...

```javascript
const tableSchema = {
    tableName: "Boats",
    partitionKeyField: "name",
    partitionKeyType: "string"
};

async function addNewBoat(boat) {
    const putRequest = dynameh.requestBuilder.buildPutInput(tableSchema, boat);
    dynameh.requestBuilder.addCondition(tableSchema, putRequest, {attribute: "name", operator: "attribute_not_exists"});
    
    try {
        await dynamodb.putItem(putRequest).promise();
    } catch (err) {
        if (err.code === "ConditionalCheckFailedException") {
            throw new Error("This boat already exists!");
        } else {
            throw err;
        }
    }
}

addNewBoat({
    name: "Boaty McBoatface",
    type: "submarine",
    autonomous: true,
    commissioned: 2016
});
```

The following conditions are available...

| condition            | # of value parameters | description |
|----------------------|-----------------------|-------------|
| =                    | 1                     | the attribute's value equals the supplied value |
| <>                   | 1                     | the attribute's value does not equal the supplied value |
| <                    | 1                     | the attribute's value is less than the supplied value |
| <=                   | 1                     | the attribute's value is less than or equal to the supplied value |
| >                    | 1                     | the attribute's value is greater than the supplied value |
| >=                   | 1                     | the attribute's value is greater than or equal to the supplied value |
| BETWEEN              | 2                     | the attribute's value is between the supplied values |
| IN                   | at least 1            | the attribute's value is in the list of supplied values |
| attribute_exists     | 0                     | the attribute has a value |
| attribute_not_exists | 0                     | the attribute does not have a value |
| attribute_type       | 1                     | the attribute's value is of the supplied type (S, SS, N, NS, B, BS, BOOL, NULL, L, M) |
| begins_with          | 1                     | the attribute's value begins with the supplied value |
| contains             | 1                     | the attribute's value is a string that contains the supplied substring or a set that contains the supplied element  |

See [the official documentation](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html) for more info.

### Filters

Filters can be added to query and scan operations to refine what objects are returned to you.  Query conditions operate on indexed fields (partition and sort) and filters operate on non-indexed fields.  Because filters are for non-indexed fields each object must be read from the database, consuming read capacity.  This makes them slower and more expensive than a well-chosen index.  A filter may still be a good idea to save bandwidth on an infrequent operation.

The filter object and the operators available are the same as conditions above.

This example scans for boats commissioned exactly 20 years ago for an anniversary announcement (an infrequent operation).

```javascript
const tableSchema = {
    tableName: "Boats",
    partitionKeyField: "name",
    partitionKeyType: "string"
};

async function getAnniversaryBoats() {
    const anniversaryYear = new Date();
    anniversaryYear.setFullYear(anniversaryYear.getFullYear() - 20);
    
    const scanRequest = dynameh.requestBuilder.buildScanInput(tableSchema);
    dynameh.requestBuilder.addFilter(tableSchema, scanRequest, {attribute: "year", operator: "=", values: [anniversaryYear]});
    
    return await dynameh.scanHelper.scanAll(dynamodb, scanRequest);
}
```

### Projections

Projections can be added to a get, batch get or query request to control what attributes are returned.  This saves bandwidth on the request.

For example...

```javascript
const tableSchema = {
    tableName: "Transactions",
    partitionKeyField: "customerId",
    partitionKeyType: "string",
    sortKeyField: "transactionId",
    sortKeyType: "string",
};

async function getTransactionDates(customerId) {
    const queryRequest = dynameh.requestBuilder.buildQueryInput(tableSchema, customerId);
    const projectedQueryRequest = dynameh.requestBuilder.addProjection(tableSchema, queryRequest, ["date"]);
    // Note that addProjection() does not change the original object.
    // queryRequest != projectedQueryRequest
    
    const queryResponse = await dynamodb.query(projectedQueryRequest).promise();
    const transactions = dynameh.responseUnwrapper.unwrapQueryOutput(queryResponse);
    return transactions.map(t => t.date);
}

getTransactionDates("BusinessFactory");
```

### Date Serialization

Date serialization can be configured by setting `dateSerializationFunction` on your `TableSchema`.  It's a function that takes in a `Date` and returns a `string` or `number`.  By default Dates are serialized as ISO-8601 strings (the only correct date format).

For example...

```javascript
const tableSchema = {
    tableName: "MyTable",
    partitionKeyField: "id",
    partitionKeyType: "number",
    dateSerializationFunction: date => date.toISOString()   // same as default
};
```
