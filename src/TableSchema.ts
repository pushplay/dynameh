export interface TableSchema {

    /**
     * The name of the table.
     */
    tableName: string;

    /**
     * The name of primary key field.
     */
    primaryKeyField: string;

    /**
     * The type of the primary key field.
     */
    primaryKeyType: "string" | "number";

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
     * locking.  If set on an item the type of the value must be "number".
     * If set and the field is not on an item during a put then
     * there must be an existing item in the table.  If set and the field
     * is present on an item then the existing value in the table must match.
     */
    versionKeyField?: string;

}
