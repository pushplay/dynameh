import * as chai from 'chai';
import { TableSchema } from './TableSchema';
import { scope } from './index';

describe("index", () => {
  describe("scope", () => {

    const defaultTableSchema: TableSchema = {
      tableName: "table",
      partitionKeyField: "primary",
      partitionKeyType: "string"
    };
    const defaultScopedDynameh = scope(defaultTableSchema);

    it("scopes buildRequestPutItem", () => {
      const serialized = defaultScopedDynameh.requestBuilder.buildRequestPutItem("Hey man nice marmot.");
      chai.assert.deepEqual(serialized, {S: "Hey man nice marmot."});
    });

    it("scopes buildGetInput", () => {
      const input = defaultScopedDynameh.requestBuilder.buildGetInput("prim");

      chai.assert.deepEqual(input, {
        TableName: "table",
        Key: {
          primary: {S: "prim"}
        }
      });
    });
  });
});
