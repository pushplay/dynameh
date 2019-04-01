import * as aws from "aws-sdk";
import * as batchHelper from "./batchHelper";
import * as concurrentHelper from "./concurrentHelper";
import * as responseUnwrapper from "./responseUnwrapper";
import {DynamoKey, DynamoKeyPair, DynamoQueryConditionOperator} from "./validation";
import {TableSchema} from "./TableSchema";
import {UpdateExpressionAction} from "./UpdateExpressionAction";
import {Condition} from "./Condition";

/**
 * An instance of Dynameh scoped to a particular TableSchema.  All calls that require
 * a TableSchema have that parameter already filled in.
 */
export interface ScopedDynameh {

    batchHelper: typeof batchHelper;

    concurrentHelper: typeof concurrentHelper;

    requestBuilder: {
        buildRequestPutItem: (item: object) => aws.DynamoDB.AttributeValue;
        buildGetInput: (partitionKeyValue: DynamoKey, sortKeyValue?: DynamoKey) => aws.DynamoDB.GetItemInput;
        buildPutInput: (item: object) => aws.DynamoDB.PutItemInput;
        buildUpdateInputFromActions: (itemToUpdate: object, ...updateActions: UpdateExpressionAction[]) => aws.DynamoDB.UpdateItemInput;
        buildDeleteInput: (itemToDelete: object) => aws.DynamoDB.DeleteItemInput;
        buildQueryInput: (partitionKeyValue: DynamoKey, sortKeyOp?: DynamoQueryConditionOperator, ...sortKeyValues: DynamoKey[]) => aws.DynamoDB.QueryInput;
        buildScanInput: (...filters: Condition[]) => aws.DynamoDB.ScanInput;
        buildBatchPutInput: (items: object[]) => aws.DynamoDB.BatchWriteItemInput;
        buildBatchDeleteInput: (keyValues: DynamoKey[] | DynamoKeyPair[]) => aws.DynamoDB.BatchWriteItemInput;
        buildBatchGetInput: (keyValues: DynamoKey[] | DynamoKeyPair[]) => aws.DynamoDB.BatchGetItemInput;
        buildTransactWriteItemsInput: (...input: (aws.DynamoDB.PutItemInput | aws.DynamoDB.DeleteItemInput | aws.DynamoDB.UpdateItemInput)[]) => aws.DynamoDB.TransactWriteItemsInput;
        addTransactWriteItems: (request: aws.DynamoDB.TransactWriteItemsInput, ...input: (aws.DynamoDB.PutItemInput | aws.DynamoDB.DeleteItemInput | aws.DynamoDB.UpdateItemInput)[]) => void;
        buildCreateTableInput: (additionalTableSchemas?: TableSchema[], readCapacity?: number, writeCapacity?: number) => aws.DynamoDB.CreateTableInput;
        buildDeleteTableInput: () => aws.DynamoDB.DeleteTableInput;
        buildUpdateTimeToLiveInput: () => aws.DynamoDB.UpdateTimeToLiveInput;
        addProjection: <T extends { ProjectionExpression?: aws.DynamoDB.ProjectionExpression, ExpressionAttributeNames?: aws.DynamoDB.ExpressionAttributeNameMap }>(projectableRequest: T, attributes: string[]) => void;
        addCondition: <T extends { ConditionExpression?: aws.DynamoDB.ConditionExpression, ExpressionAttributeNames?: aws.DynamoDB.ExpressionAttributeNameMap, ExpressionAttributeValues?: aws.DynamoDB.ExpressionAttributeValueMap }>(conditionableRequest: T, ...conditions: Condition[]) => void;
        addFilter: <T extends { FilterExpression?: aws.DynamoDB.ConditionExpression, ExpressionAttributeNames?: aws.DynamoDB.ExpressionAttributeNameMap, ExpressionAttributeValues?: aws.DynamoDB.ExpressionAttributeValueMap }>(filterableRequest: T, ...filters: Condition[]) => void;

    };

    responseUnwrapper: typeof responseUnwrapper;
}
