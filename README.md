# Dynameh
DynamoDB on Node more easier

Dynameh works with the existing [DynamoDB JavaScript API](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html) and its objects rather than wrapping them and hiding the details.  The goal is to make the existing API easier to work with where naturally possible, without ever limiting its power.  Obscure and rarely used flags are still accessible while common operations are quicker to write.

Dynameh makes it easier to: build request objects, unwrap response objects, run large batch requests and run requests concurrently that can't be run in batch.

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

See the [documentation](docs/index.html) for details on each module and its methods.

### Example 1

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

// Fetch the item from the database.
const getRequet = dynameh.requestBuilder.buildGetInput(tableSchema, "sv-650");
const getResult = await dynamodb.getItem(getRequest).promise();
const sv650 = dynameh.unwrapGetOutput(getResult);

// Update the horse power stat.
sv650.bhp = 73.4;

// Put the updated object in the database.
const putRequest = dynameh.requestBuilder.buildPutInput(tableSchema, sv650);
const putResult = await dynamodb.putItem(putRequest).promise();

```

### TableSchema

The key to easy building of requests in Dynameh is the [TableSchema](docs/interfaces/_tableschema_.tableschema.html).  This simple object defines all the extra information Dynameh needs to build requests.

#### Example 2

For a table called `MyTable` with a primary key `id` that is a number...

```json
{
  "tableName": "MyTable",
  "primaryKeyField": "id",
  "primaryKeyType": "number"
}
```

#### Example 3

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

The `version` field in **Example 3** is used for optimistic locking in `PutItem` requests.  The `version` field will be automatically incremented on the server side during a put request.  If the value of `version` sent does not match the current value in the database then the contents have changed since the last get and the optimistic lock has failed.  In that case you should get the latest version from the database and replay the update against that.
