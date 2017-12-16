import * as aws from "aws-sdk";
import {unwrapResponseItem} from "./responseUnwrapper";

/**
 * Extract the item being put from the input.
 */
export function unwrapPutInput(input: aws.DynamoDB.PutItemInput): any {
    return unwrapResponseItem({M: input.Item});
}
