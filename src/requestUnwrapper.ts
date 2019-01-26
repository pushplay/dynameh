import * as aws from "aws-sdk";
import {unwrapResponseItem} from "./responseUnwrapper";

/**
 * Extract the item being put from the input.
 * @param input The `putItem` request object.
 * @returns The item to be put to the database.
 */
export function unwrapPutInput(input: aws.DynamoDB.PutItemInput): any {
    return unwrapResponseItem({M: input.Item});
}
