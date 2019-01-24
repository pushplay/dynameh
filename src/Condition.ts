import {DynamoConditionOperator} from "./validation";

/**
 * An object representing a condition.  This is used by
 * requestBuilder.addCondition().
 * @see http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ConditionExpressions.html
 */
export interface Condition {
    /**
     * The attribute name to put the condition on.  Nested attributes
     * can be accessed using dot notation; for example `a.b.c`.  Attribute
     * names with literal dots should escape them with a backslash; for
     * example `a\.b\.c`.
     */
    attribute: string;

    /**
     * A comparison operator or function name.  All DynamoDB operators and functions
     * are supported: =, <>, <, <=, >, >=, BETWEEN, IN, attribute_exists, attribute_not_exists,
     * attribute_type, begins_with, contains, size.
     * @see http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html
     */
    operator: DynamoConditionOperator;

    /**
     * Any arguments necessary to the operator.  Can be omitted if the
     * operator requires 0 arguments.  For example `=` requires 1 value
     * and `attribute_exists` requires 0.
     */
    values?: any[];
}
