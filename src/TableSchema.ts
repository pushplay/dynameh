/**
 * Describes the DynamoDB table structure and configures settings.
 */
export interface TableSchema {

    /**
     * The name of the table.
     */
    tableName: string;

    /**
     * The name of the secondary index, when this schema represents a secondary index.
     */
    indexName?: string;

    /**
     * Additional information about the secondary index.  This is only used to fill
     * in the createTable call.
     */
    indexProperties?: {
        /**
         * "GLOBAL" for a global secondary index or "LOCAL" for a local secondary index.
         */
        type: "GLOBAL" | "LOCAL";

        /**
         * "ALL" to include all table attributes,
         * "KEYS_ONLY" to include only the primary keys or
         * "INCLUDE" to specify included attributes using 'projectedAttributes'.
         */
        projectionType: "ALL" | "KEYS_ONLY" | "INCLUDE";

        /**
         * A list of attributes to include in the secondary index.  Only specify
         * if using `projectionType="INCLUDE"`.
         */
        projectedAttributes?: string[];
    };

    /**
     * The name of the partition key field.
     */
    partitionKeyField: string;

    /**
     * The type of the partition key field.
     */
    partitionKeyType: "string" | "number";

    /**
     * The name of the sort key field.  Optional.
     * If set the sortKeyType must also be set.
     */
    sortKeyField?: string;

    /**
     * The type of the sort key field.  Optional.
     * If set the sortKeyField must also be set.
     */
    sortKeyType?: "string" | "number";

    /**
     * The name of a field to use as the version key for optimistic
     * locking.  If set on an item, the type of the value must be `number`.
     * If set and the field is not on an item during a put then
     * there must not be an existing item in the table.  If set and the field
     * is present on an item during put/update/delete then the existing value in
     * the table must match.
     */
    versionKeyField?: string;

    /**
     * The name of a field to use as the TTL (time to live) field.  Items with this field
     * set will have the value converted to seconds since 12:00:00 AM January 1st, 1970 UTC.
     * DynamoDB will automatically delete items whose `ttl` has passed (within about 48 hours).
     */
    ttlField?: string;

    /**
     * How to serialize Dates when encountered.  Defaults to ISO8601.
     * @param d the Date to serialize
     * @return a serialization of the Date
     */
    dateSerializationFunction?: (d: Date) => string | number;

}
