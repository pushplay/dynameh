import * as chai from "chai";
import {buildPutInput} from "./requestBuilder";
import {TableSchema} from "./TableSchema";
import {unwrapPutInput} from "./requestUnwrapper";

describe("unwrapPutInput", () => {
    it("unwraps an item in the put request", () => {
        const tableSchema: TableSchema = {
            tableName: "Table",
            partitionKeyField: "id",
            partitionKeyType: "number"
        };

        const item = {
            id: 111,
            aList: ["a", 123, ["nested"]],
            anObject: {
                nestedObject: {
                    nested: true
                }
            },
            classicButtonMash: "asdf",
            bool: false,
            aNull: null,
            aSet: new Set([1, 2, 3])
        };

        const request = buildPutInput(tableSchema, item);
        const unwrappedItem = unwrapPutInput(request);
        chai.assert.deepEqual(unwrappedItem, item);
    });
});
