import * as aws from "aws-sdk";
import {unwrapScanOutput} from "./responseUnwrapper";

/**
 * Paginate through a scan to collect all results.
 *
 * For large results this can use significantly *more* memory than [[scanByCallback]].
 *
 * @param dynamodb The DynamoDB client instance to use.
 * @param req The scan input.
 */
export async function scanAll(dynamodb: aws.DynamoDB, req: aws.DynamoDB.ScanInput): Promise<any[]> {
    let resp = await dynamodb.scan(req).promise();
    const results = unwrapScanOutput(resp);

    while (resp.LastEvaluatedKey) {
        req.ExclusiveStartKey = resp.LastEvaluatedKey;
        resp = await dynamodb.scan(req).promise();
        results.push(...unwrapScanOutput(resp));
    }

    return results;
}

/**
 * Paginate through a scan and pass each page of results to a callback.
 * Pagination can be aborted by returning `false` (or a Promise that
 * resolves to `false`) from the callback.
 *
 * For large results this can use significantly *less* memory than [[scanAll]].
 *
 * An example where scan is used to delete every object in a table.
 * (For large tables it's more efficient to delete and recreate the table.)
 * ```typescript
 * const scanInput = dynemeh.requestBuilder.buildScanInput(tableSchema);
 * await dynemeh.scanHelper.scanByCallback(dynamodbClient, scanInput, async items => {
 *      const keysToDelete = objectSchema.sortKeyField ?
 *          items.map(item => [item[tableSchema.partitionKeyField], item[tableSchema.sortKeyField]]) :
 *          items.map(item => item[tableSchema.partitionKeyField]);
 *      const batchDeleteInput = dynemeh.requestBuilder.buildBatchDeleteInput(tableSchema, keysToDelete);
 *      await dynemeh.batchHelper.batchWriteAll(dynamodbClient, batchDeleteInput);
 *      return true;
 *  });
 * ```
 *
 * @param dynamodb The DynamoDB client instance to use.
 * @param req The scan input.
 * @param callback A function that each page of results is passed to.  Return
 *                 `false` (or a Promise that resolves to `false`) to abort the scan.
 */
export async function scanByCallback(dynamodb: aws.DynamoDB, req: aws.DynamoDB.ScanInput, callback: (items: any[]) => boolean | Promise<boolean>): Promise<void> {
    let resp = await dynamodb.scan(req).promise();
    if (await callback(unwrapScanOutput(resp)) === false) {
        return;
    }

    while (resp.LastEvaluatedKey) {
        req.ExclusiveStartKey = resp.LastEvaluatedKey;
        resp = await dynamodb.scan(req).promise();
        if (await callback(unwrapScanOutput(resp)) === false) {
            return;
        }
    }

    return;
}

/**
 * Paginate through a scan to count all results.  This is more efficient than [[scanAll]]
 * when only the count is needed.
 *
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
