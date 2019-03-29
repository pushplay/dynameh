import * as aws from "aws-sdk";
import * as responseUnwrapper from "./responseUnwrapper";
import * as requestBuilder from "./requestBuilder";
import {Condition} from "./Condition";
import {TableSchema} from "./TableSchema";
import {checkSchema, DynamoKey, DynamoQueryConditionOperator} from "./validation";
import {UpdateExpressionAction} from "./UpdateExpressionAction";

export class FluentRequestBuilder<TRequest, TResponse, TResult> {

    private response: TResponse & { getResult: () => TResult };

    constructor(public tableSchema: TableSchema, public request: TRequest, public executor: (param: TRequest) => aws.Request<TResponse, aws.AWSError>, public resultGetter: (resp: TResponse) => TResult) {
    }

    async execute(): Promise<TResponse & { getResult: () => TResult }> {
        if (!this.response) {
            this.response = await this.executor(this.request).promise() as any;
            Object.defineProperty(this.response, "getResult", {
                value: () => this.resultGetter(this.response),
                enumerable: false
            });
        }
        return this.response;
    }

    async getResult(): Promise<TResult> {
        return this.resultGetter(await this.execute());
    }

    addProjection(attributes: string[]): FluentRequestBuilder<TRequest, TResponse, Partial<TResult>> {
        const req = requestBuilder.addProjection(this.tableSchema, this.request, attributes);
        return new FluentRequestBuilder(this.tableSchema, req, this.executor, this.resultGetter);
    }

    addCondition(...conditions: Condition[]): FluentRequestBuilder<TRequest, TResponse, TResult> {
        const req = requestBuilder.addCondition(this.tableSchema, this.request, ...conditions);
        return new FluentRequestBuilder(this.tableSchema, req, this.executor, this.resultGetter);
    }

    addFilter(...filter: Condition[]): FluentRequestBuilder<TRequest, TResponse, TResult> {
        const req = requestBuilder.addFilter(this.tableSchema, this.request, ...filter);
        return new FluentRequestBuilder(this.tableSchema, req, this.executor, this.resultGetter);
    }
}

function emptyResultGetter(): {} {
    return {};
}

export class FluentDynameh<T extends object> {

    constructor(public tableSchema: TableSchema, public client: aws.DynamoDB) {
        checkSchema(tableSchema);
    }

    getItem(partitionKeyValue: DynamoKey, sortKeyValue?: DynamoKey): FluentRequestBuilder<aws.DynamoDB.GetItemInput, aws.DynamoDB.GetItemOutput, T> {
        const req = requestBuilder.buildGetInput(this.tableSchema, partitionKeyValue, sortKeyValue);
        return new FluentRequestBuilder(this.tableSchema, req, this.client.getItem, responseUnwrapper.unwrapGetOutput);
    }

    putItem(item: T): FluentRequestBuilder<aws.DynamoDB.PutItemInput, aws.DynamoDB.PutItemOutput, {}> {
        const req = requestBuilder.buildPutInput(this.tableSchema, item);
        return new FluentRequestBuilder(this.tableSchema, req, this.client.putItem, emptyResultGetter);
    }

    updateItemFromActions(itemToUpdate: object, ...updateActions: UpdateExpressionAction[]): FluentRequestBuilder<aws.DynamoDB.UpdateItemInput, aws.DynamoDB.UpdateItemOutput, {}> {
        const req = requestBuilder.buildUpdateInputFromActions(this.tableSchema, itemToUpdate, ...updateActions);
        return new FluentRequestBuilder(this.tableSchema, req, this.client.updateItem, emptyResultGetter);
    }

    deleteItem(itemToDelete: Partial<T>): FluentRequestBuilder<aws.DynamoDB.DeleteItemInput, aws.DynamoDB.DeleteItemOutput, {}> {
        const req = requestBuilder.buildDeleteInput(this.tableSchema, itemToDelete);
        return new FluentRequestBuilder(this.tableSchema, req, this.client.deleteItem, emptyResultGetter);
    }

    query(partitionKeyValue: DynamoKey, sortKeyOp?: DynamoQueryConditionOperator, ...sortKeyValues: DynamoKey[]): FluentRequestBuilder<aws.DynamoDB.QueryInput, aws.DynamoDB.QueryOutput, T[]> {
        const req = requestBuilder.buildQueryInput(this.tableSchema, partitionKeyValue, sortKeyOp, ...sortKeyValues);
        return new FluentRequestBuilder(this.tableSchema, req, this.client.query, responseUnwrapper.unwrapQueryOutput);
        // TODO executor should get ALL query results with paging
    }

    scan(...filter: Condition[]): FluentRequestBuilder<aws.DynamoDB.ScanInput, aws.DynamoDB.ScanOutput, T[]> {
        const req = requestBuilder.buildScanInput(this.tableSchema, ...filter);
        return new FluentRequestBuilder(this.tableSchema, req, this.client.scan, responseUnwrapper.unwrapScanOutput);
        // TODO executor should get ALL scan results with paging
    }

    transactWriteItems(...input: FluentRequestBuilder<aws.DynamoDB.PutItemInput | aws.DynamoDB.DeleteItemInput | aws.DynamoDB.UpdateItemInput, any, {}>[]): FluentRequestBuilder<aws.DynamoDB.TransactWriteItemsInput, aws.DynamoDB.TransactWriteItemsOutput, {}> {
        const req = requestBuilder.buildTransactWriteItemsInput(...input.map(i => i.request));
        return new FluentRequestBuilder(this.tableSchema, req, this.client.transactWriteItems, emptyResultGetter);
    }

    createTable(additionalTableSchemas: TableSchema[] = [], readCapacity?: number, writeCapacity?: number): FluentRequestBuilder<aws.DynamoDB.CreateTableInput, aws.DynamoDB.CreateTableOutput, {}> {
        const req = requestBuilder.buildCreateTableInput([this.tableSchema, ...additionalTableSchemas], readCapacity, writeCapacity);
        return new FluentRequestBuilder(this.tableSchema, req, this.client.createTable, emptyResultGetter);
    }

    deleteTable(): FluentRequestBuilder<aws.DynamoDB.DeleteTableInput, aws.DynamoDB.DeleteTableOutput, {}> {
        const req = requestBuilder.buildDeleteTableInput(this.tableSchema);
        return new FluentRequestBuilder(this.tableSchema, req, this.client.deleteTable, emptyResultGetter);
    }

    updateTimeToLive(): FluentRequestBuilder<aws.DynamoDB.UpdateTimeToLiveInput, aws.DynamoDB.UpdateTimeToLiveOutput, {}> {
        const req = requestBuilder.buildUpdateTimeToLiveInput(this.tableSchema);
        return new FluentRequestBuilder(this.tableSchema, req, this.client.updateTimeToLive, emptyResultGetter);
    }
}
