import * as aws from "aws-sdk";
import {unwrapQueryOutput} from "./responseUnwrapper";

/**
 * Paginate through a scan to collect all results.
 * @param dynamodb The DynamoDB client instance to use.
 * @param req The scan input.
 */
export async function scanAll(dynamodb: aws.DynamoDB, req: aws.DynamoDB.ScanInput): Promise<any[]> {
    let resp = await dynamodb.scan(req).promise();
    const results = unwrapQueryOutput(resp);

    while (resp.LastEvaluatedKey) {
        req.ExclusiveStartKey = resp.LastEvaluatedKey;
        resp = await dynamodb.scan(req).promise();
        results.push(...unwrapQueryOutput(resp));
    }

    return results;
}

/**
 * Paginate through a scan to count all results.  This is more efficient than `scanAll()`
 * when only the count is needed.
 * @param dynamodb The DynamoDB client instance to use.
 * @param req The scan input.
 */
export async function scanCountAll(dynamodb: aws.DynamoDB, req: aws.DynamoDB.ScanInput): Promise<number> {
    req.Select = "Count";
    let resp = await dynamodb.scan(req).promise();
    let count = resp.Count;

    while (resp.LastEvaluatedKey) {
        req.ExclusiveStartKey = resp.LastEvaluatedKey;
        resp = await dynamodb.scan(req).promise();
        count += resp.Count;
    }

    return count;
}
