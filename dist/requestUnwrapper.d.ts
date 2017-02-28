import * as aws from "aws-sdk";
/**
 * Extract the item being put from the input.
 */
export declare function unwrapPutInput(input: aws.DynamoDB.Types.PutItemInput): any;
