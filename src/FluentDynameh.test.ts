import * as aws from "aws-sdk";
import * as chai from "chai";
import {FluentDynameh} from "./FluentDynameh";
import {TableSchema} from "./TableSchema";
import {buildRequestPutItem} from "./requestBuilder";

describe("FluentDynameh", () => {
    describe("getItem", () => {
        const tableSchema: TableSchema = {
            tableName: "TableName",
            partitionKeyField: "partition",
            partitionKeyType: "string"
        };

        function buildGetOutput(item: object): aws.DynamoDB.GetItemOutput {
            return {
                Item: buildRequestPutItem(tableSchema, item).M
            }
        }

        it("can get the result without calling execute", async () => {
            const expectedItem = {
                partition: "value1",
                foo: "bar"
            };
            const itemResponse = buildGetOutput(expectedItem);

            const fakeClient = {
                getItem: (params, callback) => {
                    return {
                        promise: () => Promise.resolve(itemResponse)
                    }
                }
            } as aws.DynamoDB;
            const dynameh = new FluentDynameh(tableSchema, fakeClient);
            const item = await dynameh.getItem("value1").getResult();
            chai.assert.deepEqual(item, expectedItem);
        });

        it("can get the result from the response", async () => {
            const expectedItem = {
                partition: "value1",
                foo: "bar"
            };
            const itemResponse = buildGetOutput(expectedItem);

            const fakeClient = {
                getItem: (params, callback) => {
                    return {
                        promise: () => Promise.resolve(itemResponse)
                    }
                }
            } as aws.DynamoDB;
            const dynameh = new FluentDynameh(tableSchema, fakeClient);
            const resp = await dynameh.getItem("value1").execute();
            const item = resp.getResult();
            chai.assert.deepEqual(item, expectedItem);
        });
    });
});
