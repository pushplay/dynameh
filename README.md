# Dynameh
DynamoDB on Node more easier

Dynameh works with the existing [DynamoDB JavaScript API](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html) and its objects rather than wrapping them and hiding the details.  The goal is to make the existing API easier to work with where naturally possible, without ever limiting its power.  Obscure and rarely used flags are still accessible while common operations are quicker to write.

Dynameh makes it easier to:
- build request objects
- unwrap response objects
- run large batch requests
- run requests concurrently that can't be run in batch
- implement optimistic locking
- configure Date serialization

## Installation

Dynameh is your typical NPM package.

```bash
npm install --save dynameh
```

## Usage

```typescript
import * as dynameh from "dynameh";
// or
const dynameh = require("dynameh");
```

See the [documentation](https://giftbit.github.io/dynameh/) for details on each module and its methods.

### A Simple Example

This example is written using async/await which is available in TypeScript and Babel.

```typescript
import * as aws from "aws-sdk";
import * as dynameh from "dynameh";

// Initialize the DynamoDB client.
const dynamodb = new aws.DynamoDB({
    apiVersion: "2012-08-10"
});

// Set up the table schema.
const tableSchema = {
    tableName: "motorcycles",
    priaryKeyField: "id",
    primaryKeyType: "string"
};

async function updateMotorcycleHorsePower(motorcycleId: string, bhp: number): Promise<void> {
    // Fetch the item from the database.
    const getRequet = dynameh.requestBuilder.buildGetInput(tableSchema, motorcycleId);
    const getResult = await dynamodb.getItem(getRequest).promise();
    const motorcycle = dynameh.unwrapGetOutput(getResult);
    
    // Update the horse power stat.
    motorcycle.bhp = bhp;
    
    // Put the updated object in the database.
    const putRequest = dynameh.requestBuilder.buildPutInput(tableSchema, motorcycle);
    const putResult = await dynamodb.putItem(putRequest).promise();
}

updateMotorcycleHorsePower("sv-650", 73.4);
```

### TableSchema

The key to easy building of requests in Dynameh is the [TableSchema](https://giftbit.github.io/dynameh/interfaces/_tableschema_.tableschema.html).  This simple object defines all the extra information Dynameh needs to build requests.

For a table called `MyTable` with a primary key `id` that is a number...

```json
{
  "tableName": "MyTable",
  "primaryKeyField": "id",
  "primaryKeyType": "number"
}
```

For a table called `MyAdvancedTable` with a primary key `id` that is a string, a range key `date` that is a number, and a version field `version`...

```json
{
  "tableName": "MyAdvancedTable",
  "primaryKeyField": "id",
  "primaryKeyType": "string",
  "sortKeyField": "date",
  "sortKeyType": "number",
  "versionKeyField": "version"
}
```

### Optimistic Locking

Optimistic locking is a strategy for preventing changes from clobbering each other.  For example two processes read from the database, make unrelated changes, and then both write to the database but the second write overwrites the first (clobbers).

The `versionKeyField` in the second TableSchema example is used for optimistic locking in `PutItem` requests.  The `version` field will be automatically incremented on the server side during a put request.  If the value of `version` sent does not match the current value in the database then the contents have changed since the last get and the optimistic lock has failed.  In that case you should get the latest version from the database and replay the update against that.

### Date Serialization

Date serialization can be configured by setting `dateSerializationFunction` on your `TableSchema`.  It's a function that takes in a `Date` and returns a `string` or `number`.  By default Dates are serialized as ISO-8601 strings.

For example...

```typescript
const tableSchema = {
    tableName: "MyTable",
    primaryKeyField: "id",
    primaryKeyType: "number",
    dateSerializationFunction: date => date.toISOString()   // same as default
}

```
