/**
 * An update action passed in to `requestBuilder.buildUpdateInputFromActions()`.
 * These actions are based upon DynamoDB's Update Expressions but more verbose.
 *
 * Every action is identified by an `action` string and also has an `attribute`
 * path. Some actions have additional fields.
 *
 * eg:
 *
 * ```
 * {
 *     action: "put",
 *     attribute: "ProductCategory",
 *     value: "Hardware"
 * }
 * ```
 *
 * @see https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.UpdateExpressions.html
 */
export type UpdateExpressionAction = UpdateExpressionPut
    | UpdateExpressionPutIfNotExists
    | UpdateExpressionRemove
    | UpdateExpressionNumberAdd
    | UpdateExpressionNumberSubtract
    | UpdateExpressionListAppend
    | UpdateExpressionListPrepend
    | UpdateExpressionListSetAtIndex
    | UpdateExpressionListRemoveAtIndex
    | UpdateExpressionSetAdd
    | UpdateExpressionSetDelete;

/**
 * Set or update the value of an attribute.
 */
export interface UpdateExpressionPut {
    action: "put";

    /**
     * The attribute name to update.  Nested attributes
     * can be accessed using dot notation; for example `a.b.c`.  Attribute
     * names with literal dots should escape them with a backslash; for
     * example `a\.b\.c`.
     * @see https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.Attributes.html
     */
    attribute: string;

    /**
     * The value to set.
     */
    value: any;
}

/**
 * Set the value of an attribute if it does not exist.
 */
export interface UpdateExpressionPutIfNotExists {
    action: "put_if_not_exists";

    /**
     * The attribute name to update.  Nested attributes
     * can be accessed using dot notation; for example `a.b.c`.  Attribute
     * names with literal dots should escape them with a backslash; for
     * example `a\.b\.c`.
     * @see https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.Attributes.html
     */
    attribute: string;

    /**
     * The value to set.
     */
    value: any;
}

/**
 * Remove an attribute.
 */
export interface UpdateExpressionRemove {
    action: "remove";

    /**
     * The attribute name to update.  Nested attributes
     * can be accessed using dot notation; for example `a.b.c`.  Attribute
     * names with literal dots should escape them with a backslash; for
     * example `a\.b\.c`.
     * @see https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.Attributes.html
     */
    attribute: string;
}

/**
 * Add to an existing numeric attribute.
 */
export interface UpdateExpressionNumberAdd {
    action: "number_add";

    /**
     * The attribute name to update.  Nested attributes
     * can be accessed using dot notation; for example `a.b.c`.  Attribute
     * names with literal dots should escape them with a backslash; for
     * example `a\.b\.c`.
     * @see https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.Attributes.html
     */
    attribute: string;

    /**
     * The number to add.
     */
    value: number;
}

/**
 * Subtract from an existing numeric attribute.
 */
export interface UpdateExpressionNumberSubtract {
    action: "number_subtract";

    /**
     * The attribute name to update.  Nested attributes
     * can be accessed using dot notation; for example `a.b.c`.  Attribute
     * names with literal dots should escape them with a backslash; for
     * example `a\.b\.c`.
     * @see https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.Attributes.html
     */
    attribute: string;

    /**
     * The number to subtract.
     */
    value: number;
}

/**
 * Add elements to the end of an existing list.
 */
export interface UpdateExpressionListAppend {
    action: "list_append";

    /**
     * The attribute name to update.  Nested attributes
     * can be accessed using dot notation; for example `a.b.c`.  Attribute
     * names with literal dots should escape them with a backslash; for
     * example `a\.b\.c`.
     * @see https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.Attributes.html
     */
    attribute: string;

    /**
     * An array of values to add.
     */
    values: any[];
}

/**
 * Add elements to the beginning of an existing list.
 */
export interface UpdateExpressionListPrepend {
    action: "list_prepend";

    /**
     * The attribute name to update.  Nested attributes
     * can be accessed using dot notation; for example `a.b.c`.  Attribute
     * names with literal dots should escape them with a backslash; for
     * example `a\.b\.c`.
     * @see https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.Attributes.html
     */
    attribute: string;

    /**
     * An array of values to add.
     */
    values: any[];
}

/**
 * Set an element at the index of an existing list.  If the
 * index does not exist it will be added at the end.
 */
export interface UpdateExpressionListSetAtIndex {
    action: "list_set_at_index";

    /**
     * The attribute name to update.  Nested attributes
     * can be accessed using dot notation; for example `a.b.c`.  Attribute
     * names with literal dots should escape them with a backslash; for
     * example `a\.b\.c`.
     * @see https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.Attributes.html
     */
    attribute: string;

    /**
     * The element to set.
     */
    value: any;

    /**
     * The index of the list.  A number.
     */
    index: number;
}

/**
 * Remove an element at the index of an existing list and
 * shift the rest down.
 */
export interface UpdateExpressionListRemoveAtIndex {
    action: "list_remove_at_index";

    /**
     * The attribute name to update.  Nested attributes
     * can be accessed using dot notation; for example `a.b.c`.  Attribute
     * names with literal dots should escape them with a backslash; for
     * example `a\.b\.c`.
     * @see https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.Attributes.html
     */
    attribute: string;

    /**
     * The index of the list.  A number.
     */
    index: number;
}

/**
 * Add an element to an existing Set.
 */
export interface UpdateExpressionSetAdd {
    action: "set_add";

    /**
     * The attribute name to update.  Nested attributes
     * can be accessed using dot notation; for example `a.b.c`.  Attribute
     * names with literal dots should escape them with a backslash; for
     * example `a\.b\.c`.
     * @see https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.Attributes.html
     */
    attribute: string;

    /**
     * A Set of strings, numbers or Buffers.
     */
    values: Set<string> | Set<number> | Set<Buffer>;
}

/**
 * Delete an element from an existing Set.
 */
export interface UpdateExpressionSetDelete {
    action: "set_delete";

    /**
     * The attribute name to update.  Nested attributes
     * can be accessed using dot notation; for example `a.b.c`.  Attribute
     * names with literal dots should escape them with a backslash; for
     * example `a\.b\.c`.
     * @see https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.Attributes.html
     */
    attribute: string;

    /**
     * A Set of strings, numbers or Buffers.
     */
    values: Set<string> | Set<number> | Set<Buffer>;
}
