import * as chai from "chai";
import {
    addCondition, addProjection, buildGetInput, buildPutInput, buildQueryInput,
    buildRequestPutItem
} from "./requestBuilder";
import {TableSchema} from "./TableSchema";

describe("requestBuilder", () => {
    describe("buildRequestPutItem", () => {
        const defaultTableSchema: TableSchema = {
            tableName: "table",
            primaryKeyField: "primary",
            primaryKeyType: "string"
        };

        it("serializes a Buffer", () => {
            const serialized = buildRequestPutItem(defaultTableSchema, Buffer.from([0x48, 0x65, 0x6c, 0x6c, 0x6f, 0x20, 0x57, 0x6f, 0x72, 0x6c, 0x64]));
            chai.assert.deepEqual(serialized, {B: "SGVsbG8gV29ybGQ="});
        });

        it("serializes a Uint8Array", () => {
            const serialized = buildRequestPutItem(defaultTableSchema, new Uint8Array([0x48, 0x65, 0x6c, 0x6c, 0x6f, 0x20, 0x57, 0x6f, 0x72, 0x6c, 0x64]));
            chai.assert.deepEqual(serialized, {B: "SGVsbG8gV29ybGQ="});
        });

        it("serializes an array of Buffers", () => {
            const serialized = buildRequestPutItem(defaultTableSchema, [
                Buffer.from([0x48, 0x65, 0x6c, 0x6c, 0x6f, 0x20, 0x57, 0x6f, 0x72, 0x6c, 0x64]),
                Buffer.from([0x4a, 0x65, 0x66, 0x66, 0x47, 0x20, 0x77, 0x61, 0x73, 0x20, 0x68, 0x65, 0x72, 0x65]),
                Buffer.from([0x6b, 0x77, 0x79, 0x6a, 0x69, 0x62, 0x6f])
            ]);
            chai.assert.deepEqual(serialized, {BS: [
                "SGVsbG8gV29ybGQ=",
                "SmVmZkcgd2FzIGhlcmU=",
                "a3d5amlibw=="
            ]});
        });

        it("serializes an array of Uint8Arrays", () => {
            const serialized = buildRequestPutItem(defaultTableSchema, [
                new Uint8Array([0x48, 0x65, 0x6c, 0x6c, 0x6f, 0x20, 0x57, 0x6f, 0x72, 0x6c, 0x64]),
                new Uint8Array([0x4a, 0x65, 0x66, 0x66, 0x47, 0x20, 0x77, 0x61, 0x73, 0x20, 0x68, 0x65, 0x72, 0x65]),
                new Uint8Array([0x6b, 0x77, 0x79, 0x6a, 0x69, 0x62, 0x6f])
            ]);
            chai.assert.deepEqual(serialized, {BS: [
                "SGVsbG8gV29ybGQ=",
                "SmVmZkcgd2FzIGhlcmU=",
                "a3d5amlibw=="
            ]});
        });

        it("marshals dates to ISO8601 strings by default", () => {
            const serialized = buildRequestPutItem(defaultTableSchema, new Date("1982-09-10"));
            chai.assert.deepEqual(serialized, {S: "1982-09-10T00:00:00.000Z"});
        });

        it("uses the set dateSerializationFunction", () => {
            const serialized = buildRequestPutItem({
                ...defaultTableSchema,
                dateSerializationFunction: d => d.getTime()
            }, new Date("1982-09-10"));
            chai.assert.deepEqual(serialized, {N: "400464000000"});
        });
    });

    describe("buildGetInput", () => {
        const defaultTableSchema: TableSchema = {
            tableName: "table",
            primaryKeyField: "primary",
            primaryKeyType: "string",
            sortKeyField: "sort",
            sortKeyType: "string"
        };

        it("builds input for a table with a hash and sort key", () => {
            const input = buildGetInput(defaultTableSchema, "prim", "so");

            chai.assert.deepEqual(input, {
                TableName: "table",
                Key: {
                    primary: {S: "prim"},
                    sort: {S: "so"}
                }
            });
        });
    });

    describe("getPutInput", () => {
        it("serializes Date TTLs", () => {
            const serialized = buildPutInput({tableName: "table",
                primaryKeyField: "primary",
                primaryKeyType: "string",
                ttlField: "ttl"
            }, {
                primary: "xxx",
                ttl: new Date('2017-08-18T21:11:29.275Z')
            });
            chai.assert.deepEqual(serialized, {
                TableName: "table",
                Item: {
                    primary: {
                        S: "xxx"
                    },
                    ttl: {
                        N: "1503090689"
                    }
                }
            });
        });
    });

    describe("buildQueryInput", () => {
        const stringSortTableSchema: TableSchema = {
            tableName: "table",
            primaryKeyField: "primary",
            primaryKeyType: "string",
            sortKeyField: "sort",
            sortKeyType: "string"
        };

        const numberSortTableSchema: TableSchema = {
            tableName: "table",
            primaryKeyField: "primary",
            primaryKeyType: "string",
            sortKeyField: "sort",
            sortKeyType: "number"
        };

        it("throws an error when no sort key is defined", () => {
            chai.assert.throws(() => {
                buildQueryInput({
                    tableName: "table",
                    primaryKeyField: "primary",
                    primaryKeyType: "string"
                }, "mah value");
            });
        });

        it("serializes a basic query without sort operator", () => {
            const input = buildQueryInput(stringSortTableSchema, "mah value");
            chai.assert.deepEqual(input, {
                TableName: "table",
                ExpressionAttributeNames: {
                    "#P": "primary"
                },
                ExpressionAttributeValues: {
                    ":p": {
                        "S": "mah value"
                    }
                },
                KeyConditionExpression: "#P = :p"
            });
        });

        it("serializes a BETWEEN query", () => {
            const input = buildQueryInput(numberSortTableSchema, "mah value", "BETWEEN", 1, 10);
            chai.assert.deepEqual(input, {
                TableName: "table",
                ExpressionAttributeNames: {
                    "#P": "primary",
                    "#S": "sort",
                },
                ExpressionAttributeValues: {
                    ":p": {
                        "S": "mah value"
                    },
                    ":v": {
                        "N": "1"
                    },
                    ":va": {
                        "N": "10"
                    }
                },
                KeyConditionExpression: "#P = :p AND #S BETWEEN :v AND :va"
            });
        });

        it("serializes a begins_with query", () => {
            const input = buildQueryInput(stringSortTableSchema, "mah value", "begins_with", "a");
            chai.assert.deepEqual(input, {
                TableName: "table",
                ExpressionAttributeNames: {
                    "#P": "primary",
                    "#S": "sort",
                },
                ExpressionAttributeValues: {
                    ":p": {
                        "S": "mah value"
                    },
                    ":v": {
                        "S": "a"
                    }
                },
                KeyConditionExpression: "#P = :p AND begins_with(#S, :v)"
            });
        });

        it("serializes a <= query", () => {
            const input = buildQueryInput(numberSortTableSchema, "mah value", "<=", 92);
            chai.assert.deepEqual(input, {
                TableName: "table",
                ExpressionAttributeNames: {
                    "#P": "primary",
                    "#S": "sort",
                },
                ExpressionAttributeValues: {
                    ":p": {
                        "S": "mah value"
                    },
                    ":v": {
                        "N": "92"
                    }
                },
                KeyConditionExpression: "#P = :p AND #S <= :v"
            });
        });
    });

    describe("addProjection", () => {
        const defaultTableSchema: TableSchema = {
            tableName: "table",
            primaryKeyField: "primary",
            primaryKeyType: "string",
            sortKeyField: "sort",
            sortKeyType: "string"
        };

        it("adds a projection to get input", () => {
            const input = buildGetInput(defaultTableSchema, "prim", "so");
            const projectedInput = addProjection(defaultTableSchema, input, ["key", "lock"]);

            chai.assert.notEqual(input, projectedInput);
            chai.assert.deepEqual(projectedInput, {
                TableName: "table",
                Key: {
                    primary: {S: "prim"},
                    sort: {S: "so"}
                },
                ExpressionAttributeNames: {
                    "#KEY": "key",
                    "#LOCK": "lock"
                },
                ProjectionExpression: "#KEY,#LOCK"
            });
        });

        it("adds a projection to an already projected expression", () => {
            const input = buildGetInput(defaultTableSchema, "prim", "so");
            const projectedInput = addProjection(defaultTableSchema, input, ["key", "lock"]);
            const projectedInput2 = addProjection(defaultTableSchema, projectedInput, ["key", "value"]);

            chai.assert.notEqual(input, projectedInput);
            chai.assert.notEqual(projectedInput, projectedInput2);
            chai.assert.deepEqual(projectedInput2, {
                TableName: "table",
                Key: {
                    primary: {S: "prim"},
                    sort: {S: "so"}
                },
                ExpressionAttributeNames: {
                    "#KEY": "key",
                    "#LOCK": "lock",
                    "#VALUE": "value"
                },
                ProjectionExpression: "#KEY,#LOCK,#VALUE"
            });
        });

        it("adds a projection to query input without key values", () => {
            const input = buildQueryInput(defaultTableSchema, "mah value", "BETWEEN", "alpha", "beta");
            const projectedInput = addProjection(defaultTableSchema, input, ["key", "lock"]);

            chai.assert.notEqual(input, projectedInput);
            chai.assert.deepEqual(projectedInput, {
                TableName: "table",
                ExpressionAttributeNames: {
                    "#P": "primary",
                    "#S": "sort",
                    "#KEY": "key",
                    "#LOCK": "lock",
                },
                ExpressionAttributeValues: {
                    ":p": {
                        "S": "mah value"
                    },
                    ":v": {
                        "S": "alpha"
                    },
                    ":va": {
                        "S": "beta"
                    }
                },
                KeyConditionExpression: `#P = :p AND #S BETWEEN :v AND :va`,
                ProjectionExpression: "#KEY,#LOCK"
            });
        });

        it("adds a projection to query input that includes key values", () => {
            const input = buildQueryInput(defaultTableSchema, "mah value", "BETWEEN", "alpha", "beta");
            const projectedInput = addProjection(defaultTableSchema, input, ["primary", "sort", "key", "lock"]);

            chai.assert.notEqual(input, projectedInput);
            chai.assert.deepEqual(projectedInput, {
                TableName: "table",
                ExpressionAttributeNames: {
                    "#P": "primary",
                    "#S": "sort",
                    "#KEY": "key",
                    "#LOCK": "lock",
                },
                ExpressionAttributeValues: {
                    ":p": {
                        "S": "mah value"
                    },
                    ":v": {
                        "S": "alpha"
                    },
                    ":va": {
                        "S": "beta"
                    }
                },
                KeyConditionExpression: `#P = :p AND #S BETWEEN :v AND :va`,
                ProjectionExpression: "#P,#S,#KEY,#LOCK"
            });
        });
    });


    describe("addCondition", () => {
        const defaultTableSchema: TableSchema = {
            tableName: "table",
            primaryKeyField: "primary",
            primaryKeyType: "string"
        };

        it("adds a condition to put input", () => {
            const input = buildPutInput(defaultTableSchema, {primary: "foo"});
            const conditionalInput = addCondition(defaultTableSchema, input, {attribute: "primary", operator: "attribute_not_exists"});

            chai.assert.notEqual(input, conditionalInput);
            chai.assert.deepEqual(conditionalInput, {
                TableName: "table",
                Item: {
                    primary: {
                        S: "foo"
                    }
                },
                ConditionExpression: "attribute_not_exists(primary)",
                ExpressionAttributeNames: {},
                ExpressionAttributeValues: {}
            });
        });

        it("adds three conditions to put input", () => {
            const input = buildPutInput(defaultTableSchema, {primary: "hick", eyes: 2, teeth: 12, ears: 2});
            const conditionalInput = addCondition(
                defaultTableSchema,
                input,
                {attribute: "eyes", operator: "<", values: [3]},
                {attribute: "teeth", operator: ">", values: [4]},
                {attribute: "ears", operator: "=", values: [2]}
            );

            chai.assert.notEqual(input, conditionalInput);
            chai.assert.deepEqual(conditionalInput, {
                TableName: "table",
                Item: {
                    primary: {
                        S: "hick"
                    },
                    eyes: {
                        N: "2"
                    },
                    teeth: {
                        N: "12"
                    },
                    ears: {
                        N: "2"
                    }
                },
                ConditionExpression: "eyes < :v AND teeth > :va AND ears = :vb",
                ExpressionAttributeNames: {},
                ExpressionAttributeValues: {
                    ":v": {
                        N: "3"
                    },
                    ":va": {
                        N: "4"
                    },
                    ":vb": {
                        N: "2"
                    }
                }
            });
        });

        it("adds conditions to input that already has conditions", () => {
            const input = buildPutInput(defaultTableSchema, {primary: "hick", eyes: 2, teeth: 12, ears: 2});
            const conditionalInput1 = addCondition(
                defaultTableSchema,
                input,
                {attribute: "eyes", operator: "<", values: [3]}
            );
            const conditionalInput2 = addCondition(
                defaultTableSchema,
                conditionalInput1,
                {attribute: "teeth", operator: ">", values: [4]}
            );
            const conditionalInput3 = addCondition(
                defaultTableSchema,
                conditionalInput2,
                {attribute: "ears", operator: "=", values: [2]}
            );

            chai.assert.deepEqual(conditionalInput3, {
                TableName: "table",
                Item: {
                    primary: {
                        S: "hick"
                    },
                    eyes: {
                        N: "2"
                    },
                    teeth: {
                        N: "12"
                    },
                    ears: {
                        N: "2"
                    }
                },
                ConditionExpression: "eyes < :v AND teeth > :va AND ears = :vb",
                ExpressionAttributeNames: {},
                ExpressionAttributeValues: {
                    ":v": {
                        N: "3"
                    },
                    ":va": {
                        N: "4"
                    },
                    ":vb": {
                        N: "2"
                    }
                }
            });
        });
    });
});
