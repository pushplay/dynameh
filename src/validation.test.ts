import * as chai from "chai";
import {checkQueryConditionOperator, checkSchema, checkSchemaItemAgreement} from "./validation";

describe("validation", () => {
    describe("checkSchema", () => {
        it("rejects a schema missing the table name", () => {
            chai.assert.throws(() => {
                checkSchema({
                    primaryKeyField: "primary",
                    primaryKeyType: "string"
                } as any);
            });
        });

        it("validates a schema with string primary key", () => {
            checkSchema({
                tableName: "tableName",
                primaryKeyField: "primary",
                primaryKeyType: "string"
            });
        });

        it("validates a schema with number primary key", () => {
            checkSchema({
                tableName: "tableName",
                primaryKeyField: "primary",
                primaryKeyType: "string"
            });
        });

        it("rejects a schema missing the primary key field", () => {
            chai.assert.throws(() => {
                checkSchema({
                    tableName: "tableName",
                    primaryKeyType: "string"
                } as any);
            });
        });

        it("rejects a schema missing the primary key field", () => {
            chai.assert.throws(() => {
                checkSchema({
                    tableName: "tableName",
                    primaryKeyField: "primary",
                } as any);
            });
        });

        it("rejects a schema with boolean primary key", () => {
            chai.assert.throws(() => {
                checkSchema({
                    tableName: "tableName",
                    primaryKeyField: "primary",
                    primaryKeyType: "boolean"
                } as any);
            });
        });

        it("rejects a schema with object primary key", () => {
            chai.assert.throws(() => {
                checkSchema({
                    tableName: "tableName",
                    primaryKeyField: "primary",
                    primaryKeyType: "object"
                } as any);
            });
        });

        it("validates a schema with string sort key", () => {
            checkSchema({
                tableName: "tableName",
                primaryKeyField: "primary",
                primaryKeyType: "string",
                sortKeyField: "sort",
                sortKeyType: "string"
            });
        });

        it("validates a schema with number sort key", () => {
            checkSchema({
                tableName: "tableName",
                primaryKeyField: "primary",
                primaryKeyType: "string",
                sortKeyField: "sort",
                sortKeyType: "number"
            });
        });

        it("rejects a schema with boolean sort key", () => {
            chai.assert.throws(() => {
                checkSchema({
                    tableName: "tableName",
                    primaryKeyField: "primary",
                    primaryKeyType: "string",
                    sortKeyField: "sort",
                    sortKeyType: "boolean"
                } as any);
            });
        });

        it("rejects a schema with object sort key", () => {
            chai.assert.throws(() => {
                checkSchema({
                    tableName: "tableName",
                    primaryKeyField: "primary",
                    primaryKeyType: "string",
                    sortKeyField: "sort",
                    sortKeyType: "object"
                } as any);
            });
        });

        it("rejects a schema with sort key field without type", () => {
            chai.assert.throws(() => {
                checkSchema({
                    tableName: "tableName",
                    primaryKeyField: "primary",
                    primaryKeyType: "string",
                    sortKeyField: "sort"
                } as any);
            });
        });

        it("rejects a schema with sort key type without field", () => {
            chai.assert.throws(() => {
                checkSchema({
                    tableName: "tableName",
                    primaryKeyField: "primary",
                    primaryKeyType: "string",
                    sortKeyType: "object"
                } as any);
            });
        });
    });

    describe("checkSchemaItemAgreement", () => {
        it("validates a schema with string", () => {
            checkSchemaItemAgreement({
                tableName: "tableName",
                primaryKeyField: "primary",
                primaryKeyType: "string"
            }, {
                primary: "primary value"
            });
        });

        it("validates a schema with number", () => {
            checkSchemaItemAgreement({
                tableName: "tableName",
                primaryKeyField: "primary",
                primaryKeyType: "number"
            }, {
                primary: 789
            });
        });

        it("rejects a schema with string given number", () => {
            chai.assert.throws(() => {
                checkSchemaItemAgreement({
                    tableName: "tableName",
                    primaryKeyField: "primary",
                    primaryKeyType: "string"
                }, {
                    primary: 3456234
                });
            });
        });

        it("rejects a schema with string given object", () => {
            chai.assert.throws(() => {
                checkSchemaItemAgreement({
                    tableName: "tableName",
                    primaryKeyField: "primary",
                    primaryKeyType: "string"
                }, {
                    primary: {}
                });
            });
        });

        it("validates a schema with string and string sort key", () => {
            checkSchemaItemAgreement({
                tableName: "tableName",
                primaryKeyField: "primary",
                primaryKeyType: "string",
                sortKeyField: "sort",
                sortKeyType: "string"
            }, {
                primary: "primary value",
                sort: "sort value"
            });
        });

        it("validates a schema with string and number sort key", () => {
            checkSchemaItemAgreement({
                tableName: "tableName",
                primaryKeyField: "primary",
                primaryKeyType: "string",
                sortKeyField: "sort",
                sortKeyType: "number"
            }, {
                primary: "primary value",
                sort: 67852345
            });
        });

        it("rejects a schema with string and string sort key when the item is missing the sort", () => {
            chai.assert.throws(() => {
                checkSchemaItemAgreement({
                    tableName: "tableName",
                    primaryKeyField: "primary",
                    primaryKeyType: "string",
                    sortKeyField: "sort",
                    sortKeyType: "string"
                }, {
                    primary: "primary value"
                });
            });
        });

        it("rejects a schema with string and string sort key when the item has number sort", () => {
            chai.assert.throws(() => {
                checkSchemaItemAgreement({
                    tableName: "tableName",
                    primaryKeyField: "primary",
                    primaryKeyType: "string",
                    sortKeyField: "sort",
                    sortKeyType: "string"
                }, {
                    primary: "primary value",
                    sort: 67852345
                });
            });
        });

        it("validates a schema with version key field", () => {
            checkSchemaItemAgreement({
                tableName: "tableName",
                primaryKeyField: "primary",
                primaryKeyType: "string",
                versionKeyField: "version"
            }, {
                primary: "primary value",
                version: 0
            });
        });

        it("validates a schema with version key field when the item does not have it set", () => {
            checkSchemaItemAgreement({
                tableName: "tableName",
                primaryKeyField: "primary",
                primaryKeyType: "string",
                versionKeyField: "version"
            }, {
                primary: "primary value"
            });
        });

        it("rejects a schema with version key field when the item has the wrong type on the value", () => {
            chai.assert.throws(() => {
                checkSchemaItemAgreement({
                    tableName: "tableName",
                    primaryKeyField: "primary",
                    primaryKeyType: "string",
                    versionKeyField: "version"
                }, {
                    primary: "primary value",
                    version: "this ain't legal"
                });
            });
        });
    });

    describe("checkQueryConditionOperator", () => {
        it("requires the operator to be defined", () => {
            chai.assert.throws(() => {
                checkQueryConditionOperator(undefined);
            });
        });

        it("validates operators that can be used in a query KeyConditionExpression", () => {
            checkQueryConditionOperator("=");
            checkQueryConditionOperator("<");
            checkQueryConditionOperator("<=");
            checkQueryConditionOperator(">");
            checkQueryConditionOperator(">=");
            checkQueryConditionOperator("BETWEEN");
            checkQueryConditionOperator("begins_with");
        });

        it("doesn't validate operators that can't be used in a query KeyConditionExpression", () => {
            chai.assert.throws(() => {
                // Not to be confused with "=".
                checkQueryConditionOperator("==" as any);
            });
            chai.assert.throws(() => {
                // This is a valid comparison operator.
                checkQueryConditionOperator("<>" as any);
            });
        });
    });
});
