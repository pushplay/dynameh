import * as chai from "chai";
import {checkCondition, checkSchema, checkSchemaItemAgreement} from "./validation";

describe("validation", () => {
    describe("checkSchema", () => {
        it("rejects a schema missing the table name", () => {
            chai.assert.throws(() => {
                checkSchema({
                    partitionKeyField: "primary",
                    partitionKeyType: "string"
                } as any);
            });
        });

        it("validates a schema with string primary key", () => {
            checkSchema({
                tableName: "tableName",
                partitionKeyField: "primary",
                partitionKeyType: "string"
            });
        });

        it("validates a schema with number primary key", () => {
            checkSchema({
                tableName: "tableName",
                partitionKeyField: "primary",
                partitionKeyType: "string"
            });
        });

        it("rejects a schema missing the primary key field", () => {
            chai.assert.throws(() => {
                checkSchema({
                    tableName: "tableName",
                    partitionKeyType: "string"
                } as any);
            });
        });

        it("rejects a schema missing the primary key field", () => {
            chai.assert.throws(() => {
                checkSchema({
                    tableName: "tableName",
                    partitionKeyField: "primary",
                } as any);
            });
        });

        it("rejects a schema with boolean primary key", () => {
            chai.assert.throws(() => {
                checkSchema({
                    tableName: "tableName",
                    partitionKeyField: "primary",
                    partitionKeyType: "boolean"
                } as any);
            });
        });

        it("rejects a schema with object primary key", () => {
            chai.assert.throws(() => {
                checkSchema({
                    tableName: "tableName",
                    partitionKeyField: "primary",
                    partitionKeyType: "object"
                } as any);
            });
        });

        it("validates a schema with string sort key", () => {
            checkSchema({
                tableName: "tableName",
                partitionKeyField: "primary",
                partitionKeyType: "string",
                sortKeyField: "sort",
                sortKeyType: "string"
            });
        });

        it("validates a schema with number sort key", () => {
            checkSchema({
                tableName: "tableName",
                partitionKeyField: "primary",
                partitionKeyType: "string",
                sortKeyField: "sort",
                sortKeyType: "number"
            });
        });

        it("rejects a schema with boolean sort key", () => {
            chai.assert.throws(() => {
                checkSchema({
                    tableName: "tableName",
                    partitionKeyField: "primary",
                    partitionKeyType: "string",
                    sortKeyField: "sort",
                    sortKeyType: "boolean"
                } as any);
            });
        });

        it("rejects a schema with object sort key", () => {
            chai.assert.throws(() => {
                checkSchema({
                    tableName: "tableName",
                    partitionKeyField: "primary",
                    partitionKeyType: "string",
                    sortKeyField: "sort",
                    sortKeyType: "object"
                } as any);
            });
        });

        it("rejects a schema with sort key field without type", () => {
            chai.assert.throws(() => {
                checkSchema({
                    tableName: "tableName",
                    partitionKeyField: "primary",
                    partitionKeyType: "string",
                    sortKeyField: "sort"
                } as any);
            });
        });

        it("rejects a schema with sort key type without field", () => {
            chai.assert.throws(() => {
                checkSchema({
                    tableName: "tableName",
                    partitionKeyField: "primary",
                    partitionKeyType: "string",
                    sortKeyType: "object"
                } as any);
            });
        });
    });

    describe("checkSchemaItemAgreement", () => {
        it("validates a schema with string", () => {
            checkSchemaItemAgreement({
                tableName: "tableName",
                partitionKeyField: "primary",
                partitionKeyType: "string"
            }, {
                primary: "primary value"
            });
        });

        it("validates a schema with number", () => {
            checkSchemaItemAgreement({
                tableName: "tableName",
                partitionKeyField: "primary",
                partitionKeyType: "number"
            }, {
                primary: 789
            });
        });

        it("validates a schema with number and value 0", () => {
            checkSchemaItemAgreement({
                tableName: "tableName",
                partitionKeyField: "primary",
                partitionKeyType: "number"
            }, {
                primary: 0
            });
        });

        it("rejects a schema with string given number", () => {
            chai.assert.throws(() => {
                checkSchemaItemAgreement({
                    tableName: "tableName",
                    partitionKeyField: "primary",
                    partitionKeyType: "string"
                }, {
                    primary: 3456234
                });
            });
        });

        it("rejects a schema with string given object", () => {
            chai.assert.throws(() => {
                checkSchemaItemAgreement({
                    tableName: "tableName",
                    partitionKeyField: "primary",
                    partitionKeyType: "string"
                }, {
                    primary: {}
                });
            });
        });

        it("validates a schema with string and string sort key", () => {
            checkSchemaItemAgreement({
                tableName: "tableName",
                partitionKeyField: "primary",
                partitionKeyType: "string",
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
                partitionKeyField: "primary",
                partitionKeyType: "string",
                sortKeyField: "sort",
                sortKeyType: "number"
            }, {
                primary: "primary value",
                sort: 67852345
            });
        });

        it("validates a schema with string and number sort key of value 0", () => {
            checkSchemaItemAgreement({
                tableName: "tableName",
                partitionKeyField: "primary",
                partitionKeyType: "string",
                sortKeyField: "sort",
                sortKeyType: "number"
            }, {
                primary: "primary value",
                sort: 0
            });
        });

        it("rejects a schema with string and string sort key when the item is missing the sort", () => {
            chai.assert.throws(() => {
                checkSchemaItemAgreement({
                    tableName: "tableName",
                    partitionKeyField: "primary",
                    partitionKeyType: "string",
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
                    partitionKeyField: "primary",
                    partitionKeyType: "string",
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
                partitionKeyField: "primary",
                partitionKeyType: "string",
                versionKeyField: "version"
            }, {
                primary: "primary value",
                version: 0
            });
        });

        it("validates a schema with version key field when the item does not have it set", () => {
            checkSchemaItemAgreement({
                tableName: "tableName",
                partitionKeyField: "primary",
                partitionKeyType: "string",
                versionKeyField: "version"
            }, {
                primary: "primary value"
            });
        });

        it("rejects a schema with version key field when the item has the wrong type on the value", () => {
            chai.assert.throws(() => {
                checkSchemaItemAgreement({
                    tableName: "tableName",
                    partitionKeyField: "primary",
                    partitionKeyType: "string",
                    versionKeyField: "version"
                }, {
                    primary: "primary value",
                    version: "this ain't legal"
                });
            });
        });

        it("validates a schema with ttlField and a Date is set", () => {
            checkSchemaItemAgreement({
                tableName: "tableName",
                partitionKeyField: "primary",
                partitionKeyType: "string",
                ttlField: "ttl"
            }, {
                primary: "primary value",
                ttl: new Date()
            });
        });

        it("validates a schema with ttlField and a number set", () => {
            checkSchemaItemAgreement({
                tableName: "tableName",
                partitionKeyField: "primary",
                partitionKeyType: "string",
                ttlField: "ttl"
            }, {
                primary: "primary value",
                ttl: new Date().getTime() / 1000
            });
        });

        it("validates a schema with ttlField and none is set", () => {
            checkSchemaItemAgreement({
                tableName: "tableName",
                partitionKeyField: "primary",
                partitionKeyType: "string",
                ttlField: "ttl"
            }, {
                primary: "primary value",
                ttl: null
            });
        });

        it("rejects a schema with ttlField and a string set", () => {
            chai.assert.throws(() => {
                checkSchemaItemAgreement({
                    tableName: "tableName",
                    partitionKeyField: "primary",
                    partitionKeyType: "string",
                    ttlField: "ttl"
                }, {
                    primary: "primary value",
                    ttl: "2017-08-17T23:20:05.743Z"
                });
            });
        });

        it("rejects a schema with ttlField and an object set", () => {
            chai.assert.throws(() => {
                checkSchemaItemAgreement({
                    tableName: "tableName",
                    partitionKeyField: "primary",
                    partitionKeyType: "string",
                    ttlField: "ttl"
                }, {
                    primary: "primary value",
                    ttl: {
                        key: "well this is clearly dumb"
                    }
                });
            });
        });
    });

    describe("checkCondition", () => {
        it("requires the operator to be defined", () => {
            chai.assert.throws(() => {
                checkCondition({
                    attribute: "a",
                    operator: null,
                }, "query");
            });
        });

        it("validates operators that can be used in a query KeyConditionExpression", () => {
            checkCondition({attribute: "a", operator: "=", values: [1]}, "query");
            checkCondition({attribute: "a", operator: "<", values: [1]}, "query");
            checkCondition({attribute: "a", operator: "<=", values: [1]}, "query");
            checkCondition({attribute: "a", operator: ">", values: [1]}, "query");
            checkCondition({attribute: "a", operator: ">=", values: [1]}, "query");
            checkCondition({attribute: "a", operator: "BETWEEN", values: [1, 2]}, "query");
            checkCondition({attribute: "a", operator: "begins_with", values: ["x"]}, "query");
        });

        it("validates operators that can be used in a ConditionExpression", () => {
            checkCondition({attribute: "a", operator: "=", values: [1]}, "default");
            checkCondition({attribute: "a", operator: "<>", values: [1]}, "default");
            checkCondition({attribute: "a", operator: "<", values: [1]}, "default");
            checkCondition({attribute: "a", operator: "<=", values: [1]}, "default");
            checkCondition({attribute: "a", operator: ">", values: [1]}, "default");
            checkCondition({attribute: "a", operator: ">=", values: [1]}, "default");
            checkCondition({attribute: "a", operator: "BETWEEN", values: [1, 2]}, "default");
            checkCondition({attribute: "a", operator: "attribute_exists"}, "default");
            checkCondition({attribute: "a", operator: "attribute_not_exists"}, "default");
            checkCondition({attribute: "a", operator: "attribute_type", values: ["S"]}, "default");
            checkCondition({attribute: "a", operator: "begins_with", values: ["x"]}, "default");
            checkCondition({attribute: "a", operator: "contains", values: ["x"]}, "default");
            checkCondition({attribute: "a", operator: "size"}, "default");
        });

        it("rejects conditions with the wrong number of arguments", () => {
            chai.assert.throws(() => {
                checkCondition({attribute: "a", operator: "="}, "default");
            });
            chai.assert.throws(() => {
                checkCondition({attribute: "a", operator: "=", values: [1, 2]}, "default");
            });
            chai.assert.throws(() => {
                checkCondition({attribute: "a", operator: "BETWEEN"}, "default");
            });
            chai.assert.throws(() => {
                checkCondition({attribute: "a", operator: "BETWEEN", values: [1]}, "default");
            });
            chai.assert.throws(() => {
                checkCondition({attribute: "a", operator: "BETWEEN", values: [1, 2, 3]}, "default");
            });
        });

        it("rejects operators that can't be used in a query KeyConditionExpression", () => {
            chai.assert.throws(() => {
                // Not to be confused with "=".
                checkCondition({attribute: "a", operator: "==" as any, values: [1]}, "query");
            });
            chai.assert.throws(() => {
                // This is a valid comparison operator.
                checkCondition({attribute: "a", operator: "<>", values: [1]}, "query");
            });
        });
    });
});
