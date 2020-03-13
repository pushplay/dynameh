import * as aws from "aws-sdk";
import {unwrapQueryOutput} from "./responseUnwrapper";

/**
 * Paginate through a query to collect all results.
 *
 * For large results this can use significantly *more* memory than [[queryByCallback]].
 *
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
 * Paginate through a query and pass each page of results to a callback.
 * Pagination can be aborted by returning `false` (or a Promise that
 * resolves to `false`) from the callback.
 *
 * For large results this can use significantly *less* memory than [[queryAll]].
 *
 * @param dynamodb The DynamoDB client instance to use.
 * @param req The query input.
 * @param callback A function that each page of results is passed to.  Return
 *                 `false` (or a Promise that resolves to `false`) to abort the query.
 */
export async function queryByCallback(dynamodb: aws.DynamoDB, req: aws.DynamoDB.QueryInput, callback: (items: any[]) => boolean | Promise<boolean>): Promise<void> {
    let resp = await dynamodb.query(req).promise();
    if (await callback(unwrapQueryOutput(resp)) === false) {
        return;
    }

    while (resp.LastEvaluatedKey) {
        req.ExclusiveStartKey = resp.LastEvaluatedKey;
        resp = await dynamodb.query(req).promise();
        if (await callback(unwrapQueryOutput(resp)) === false) {
            return;
        }
    }

    return;
}

/**
 * Paginate through a query to count all results.  This is more efficient than [[queryAll]]
 * when only the count is needed.
 *
 * @param dynamodb The DynamoDB client instance to use.
 * @param req The query input.
 */
export async function queryCountAll(dynamodb: aws.DynamoDB, req: aws.DynamoDB.QueryInput): Promise<number> {
    req.Select = "COUNT";
    let resp = await dynamodb.query(req).promise();
    let count = resp.Count;

    while (resp.LastEvaluatedKey) {
        req.ExclusiveStartKey = resp.LastEvaluatedKey;
        resp = await dynamodb.query(req).promise();
        count += resp.Count;
    }

    return count;
}
