import * as chai from "chai";
import {buildQueryInput, buildRequestPutItem} from "./requestBuilder";
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

    describe("buildQueryInput", () => {
        const defaultTableSchema: TableSchema = {
            tableName: "table",
            primaryKeyField: "primary",
            primaryKeyType: "string",
            sortKeyField: "sort",
            sortKeyType: "string"
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
            const input = buildQueryInput(defaultTableSchema, "mah value");
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
                KeyConditionExpression: `#P = :p`
            });
        });

        it("serializes a BETWEEN query", () => {
            const input = buildQueryInput(defaultTableSchema, "mah value", "BETWEEN", "alpha", "beta");
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
                    ":s": {
                        "S": "alpha"
                    },
                    ":sa": {
                        "S": "beta"
                    }
                },
                KeyConditionExpression: `#P = :p AND #S BETWEEN :s AND :sa`
            });
        });
    });
});
