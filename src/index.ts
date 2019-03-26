import * as batchHelper from "./batchHelper";
import * as concurrentHelper from "./concurrentHelper";
import * as requestBuilder from "./requestBuilder";
import * as requestUnwrapper from "./requestUnwrapper";
import * as responseUnwrapper from "./responseUnwrapper";
import {TableSchema} from "./TableSchema";
import {ScopedDynameh} from "./ScopedDynameh";
import {DynamoKey} from "./validation";

export * from "./Condition";
export * from "./ScopedDynameh";
export * from "./TableSchema";
export * from "./UpdateExpressionAction";

export {batchHelper, concurrentHelper, requestBuilder, requestUnwrapper, responseUnwrapper};

/**
 * Create an instance of Dynameh scoped to the given TableSchema.  All calls that require
 * a TableSchema have that parameter already filled in.
 * @param tableSchema
 */
export function scope(tableSchema: TableSchema): ScopedDynameh {
    return {
        requestBuilder: {
            buildRequestPutItem: (item) => requestBuilder.buildRequestPutItem(tableSchema, item),
            buildGetInput: (partitionKeyValue, sortKeyValue?) => requestBuilder.buildGetInput(tableSchema, partitionKeyValue, sortKeyValue),
            buildPutInput: (item) => requestBuilder.buildPutInput(tableSchema, item),
            buildUpdateInputFromActions: (itemToUpdate, ...updateActions) => requestBuilder.buildUpdateInputFromActions(tableSchema, itemToUpdate, ...updateActions),
            buildDeleteInput: (itemToDelete) => requestBuilder.buildDeleteInput(tableSchema, itemToDelete),
            buildDeleteTableInput: () => requestBuilder.buildDeleteTableInput(tableSchema),
            buildQueryInput: (partitionKeyValue: DynamoKey, sortKeyOp?, ...sortKeyValues: DynamoKey[]) => requestBuilder.buildQueryInput(tableSchema, partitionKeyValue, sortKeyOp, ...sortKeyValues),
            buildScanInput: (...filters) => requestBuilder.buildScanInput(tableSchema, ...filters),
            buildBatchPutInput: (items) => requestBuilder.buildBatchPutInput(tableSchema, items),
            buildBatchDeleteInput: (keyValues) => requestBuilder.buildBatchDeleteInput(tableSchema, keyValues),
            buildBatchGetInput: (keyValues) => requestBuilder.buildBatchGetInput(tableSchema, keyValues),
            buildTransactWriteItemsInput: (...input) => requestBuilder.buildTransactWriteItemsInput(...input),
            buildCreateTableInput: (additionalTableSchemas: TableSchema[] = [], readCapacity: number = 1, writeCapacity: number = 1) => requestBuilder.buildCreateTableInput([tableSchema, ...additionalTableSchemas], readCapacity, writeCapacity),
            buildUpdateTimeToLiveInput: () => requestBuilder.buildUpdateTimeToLiveInput(tableSchema),
            addProjection: (projectableRequest, attributes) => requestBuilder.addProjection(tableSchema, projectableRequest, attributes),
            addCondition: (conditionableRequest, ...conditions) => requestBuilder.addCondition(tableSchema, conditionableRequest, ...conditions),
            addFilter: (filterableRequest, ...filters) => requestBuilder.addFilter(tableSchema, filterableRequest, ...filters)
        },
        responseUnwrapper: responseUnwrapper,
        batchHelper: batchHelper,
        concurrentHelper: concurrentHelper
    };
}
