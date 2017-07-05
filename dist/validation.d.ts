import { TableSchema } from "./TableSchema";
export declare type DynamoKey = string | number;
export declare type DynamoKeyPair = [DynamoKey, DynamoKey];
export declare function checkSchema(tableSchema: TableSchema): void;
/**
 * Assumes checkSchema(tableSchema) has already been run.
 */
export declare function checkSchemaKeyAgreement(tableSchema: TableSchema, primaryKeyValue: DynamoKey, sortKeyValue?: DynamoKey): void;
/**
 * Assumes checkSchema(tableSchema) has already been run.
 */
export declare function checkSchemaKeysAgreement(tableSchema: TableSchema, keyValues: DynamoKey[] | DynamoKeyPair[]): void;
/**
 * Assumes checkSchema(tableSchema) has already been run.
 */
export declare function checkSchemaItemAgreement(tableSchema: TableSchema, item: Object): void;
/**
 * Assumes checkSchema(tableSchema) has already been run.
 */
export declare function checkSchemaItemsAgreement(tableSchema: TableSchema, items: Object[]): void;
