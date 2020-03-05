import * as aws from "aws-sdk";
import {unwrapQueryOutput} from "./responseUnwrapper";

/**
 * Paginate through a query to collect all results.
 * @param dynamodb The DynamoDB client instance to use.
 * @param req The query input.
 */
export async function queryAll(dynamodb: aws.DynamoDB, req: aws.DynamoDB.QueryInput): Promise<any[]> {
    let resp = await dynamodb.query(req).promise();
    const results = unwrapQueryOutput(resp);

    while (resp.LastEvaluatedKey) {
        req.ExclusiveStartKey = resp.LastEvaluatedKey;
        resp = await dynamodb.query(req).promise();
        results.push(...unwrapQueryOutput(resp));
    }

    return results;
}

/**
 * Paginate through a query to count all results.  This is more efficient than `queryAll()`
 * when only the count is needed.
 * @param dynamodb The DynamoDB client instance to use.
 * @param req The query input.
 */
export async function queryCountAll(dynamodb: aws.DynamoDB, req: aws.DynamoDB.QueryInput): Promise<number> {
    req.Select = "Count";
    let resp = await dynamodb.query(req).promise();
    let count = resp.Count;

    while (resp.LastEvaluatedKey) {
        req.ExclusiveStartKey = resp.LastEvaluatedKey;
        resp = await dynamodb.query(req).promise();
        count += resp.Count;
    }

    return count;
}
