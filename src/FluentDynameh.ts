import * as aws from "aws-sdk";
import * as responseUnwrapper from "./responseUnwrapper";
import * as requestBuilder from "./requestBuilder";
import {Condition} from "./Condition";
import {TableSchema} from "./TableSchema";
import {checkSchema, DynamoKey, DynamoQueryConditionOperator} from "./validation";
import {UpdateExpressionAction} from "./UpdateExpressionAction";

export class FluentRequestBuilder<TRequest, TResponse, TResult> {

    private response: TResponse & { getResult: () => TResult };

    constructor(
        public readonly tableSchema: TableSchema,
        public readonly request: TRequest,
        private readonly executor: (param: TRequest) => aws.Request<TResponse, aws.AWSError>,
        private readonly resultGetter: (resp: TResponse) => TResult
    ) {
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

    addProjection(attributes: string[]): this {
        requestBuilder.addProjection(this.tableSchema, this.request, attributes);
        return this;
    }

    addCondition(...conditions: Condition[]): this {
        requestBuilder.addCondition(this.tableSchema, this.request, ...conditions);
        return this;
    }

    addFilter(...filter: Condition[]): this {
        requestBuilder.addFilter(this.tableSchema, this.request, ...filter);
        return this;
    }
}

export class FluentTransactWriteItemsBuilder<T extends object> {

    public readonly request: aws.DynamoDB.TransactWriteItemsInput;
    private response: aws.DynamoDB.TransactWriteItemsOutput;

    constructor(public readonly tableSchema: TableSchema, public readonly client: aws.DynamoDB) {
    }

    async execute(): Promise<aws.DynamoDB.TransactWriteItemsOutput> {
        if (!this.response) {
            this.response = await this.client.transactWriteItems(this.request).promise();
        }
        return this.response;
    }

    putItem(item: T): FluentRequestBuilder<aws.DynamoDB.PutItemInput, void, {}> {
        const req = requestBuilder.buildPutInput(this.tableSchema, item);
        requestBuilder.addTransactWriteItemsInput(this.request, req);
        return new FluentRequestBuilder(this.tableSchema, req, notExecutable, emptyResultGetter);
    }

    updateItemFromActions(itemToUpdate: object, ...updateActions: UpdateExpressionAction[]): FluentRequestBuilder<aws.DynamoDB.UpdateItemInput, void, {}> {
        const req = requestBuilder.buildUpdateInputFromActions(this.tableSchema, itemToUpdate, ...updateActions);
        requestBuilder.addTransactWriteItemsInput(this.request, req);
        return new FluentRequestBuilder(this.tableSchema, req, notExecutable, emptyResultGetter);
    }

    deleteItem(itemToDelete: Partial<T>): FluentRequestBuilder<aws.DynamoDB.DeleteItemInput, void, {}> {
        const req = requestBuilder.buildDeleteInput(this.tableSchema, itemToDelete);
        requestBuilder.addTransactWriteItemsInput(this.request, req);
        return new FluentRequestBuilder(this.tableSchema, req, notExecutable, emptyResultGetter);
    }
}

function emptyResultGetter(): {} {
    return {};
}

function notExecutable(): any {
    throw new Error("This builder is not executable because it is part of a transaction.");
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

    transactWriteItems(): FluentTransactWriteItemsBuilder<T> {
        return new FluentTransactWriteItemsBuilder(this.tableSchema, this.client);
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
