import * as chai from "chai";
import {buildRequestPutItem} from "./requestBuilder";

describe("requestBuilder", () => {
    describe("buildRequestPutItem", () => {
        it("marshalls dates to ISO8601 strings by default", () => {
            const serialized = buildRequestPutItem({
                    tableName: "table",
                    primaryKeyField: "prim",
                    primaryKeyType: "string"
                }, new Date("1982-09-10"));
            chai.assert.deepEqual(serialized, {S: "1982-09-10T00:00:00.000Z"});
        });

        it("uses the set dateSerializationFunction", () => {
            const serialized = buildRequestPutItem({
                tableName: "table",
                primaryKeyField: "prim",
                primaryKeyType: "string",
                dateSerializationFunction: d => d.getTime()
            }, new Date("1982-09-10"));
            chai.assert.deepEqual(serialized, {N: "400464000000"});
        });
    });
});
