# Dynameh
DynamoDB on Node more easier

Dynameh works with the existing [DynamoDB JavaScript API](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html) and its objects rather than wrapping them and hiding the details.  The goal is to make the existing API easier to work with where naturally possible, without ever limiting its power.  Obscure and rarely used flags are still accessible while common operations are quicker to write.

Dynameh makes it easier to: build request objects, unwrap response objects, run large batch requests and run requests concurrently that can't be run in batch.

## Installation

Dynameh is your typical NPM package.

```bash
npm install --save dynameh
```

## Building request objects

