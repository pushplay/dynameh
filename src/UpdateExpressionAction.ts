export type UpdateExpressionAction = UpdateExpressionPut
    | UpdateExpressionPutIfNotExists
    | UpdateExpressionDelete
    | UpdateExpressionNumberAdd
    | UpdateExpressionNumberSubtract
    | UpdateExpressionListAppend
    | UpdateExpressionListPrepend
    | UpdateExpressionListSetAtIndex
    | UpdateExpressionListDeleteAtIndex
    | UpdateExpressionSetAdd
    | UpdateExpressionSetDelete;

export interface UpdateExpressionPut {
    action: "put";
    attribute: string;
    value: any;
}

export interface UpdateExpressionPutIfNotExists {
    action: "put_if_not_exists";
    attribute: string;
    value: any;
}

export interface UpdateExpressionDelete {
    action: "delete";
    attribute: string;
}

export interface UpdateExpressionNumberAdd {
    action: "number_add";
    attribute: string;
    value: number;
}

export interface UpdateExpressionNumberSubtract {
    action: "number_subtract";
    attribute: string;
    value: number;
}

export interface UpdateExpressionListAppend {
    action: "list_append";
    attribute: string;
    values: any[];
}

export interface UpdateExpressionListPrepend {
    action: "list_prepend";
    attribute: string;
    values: any[];
}

export interface UpdateExpressionListSetAtIndex {
    action: "list_set_at_index";
    attribute: string;
    value: any;
    index: number;
}

export interface UpdateExpressionListDeleteAtIndex {
    action: "list_delete_at_index";
    attribute: string;
    index: number;
}

export interface UpdateExpressionSetAdd {
    action: "set_add";
    attribute: string;
    values: Set<string> | Set<number> | Set<Buffer>;
}

export interface UpdateExpressionSetDelete {
    action: "set_delete";
    attribute: string;
    values: Set<string> | Set<number> | Set<Buffer>;
}

export module UpdateExpressionAction {
    export function getUpdateExpressionClauseKey(action: UpdateExpressionAction): "SET" | "REMOVE" | "ADD" | "DELETE" {
        switch (action.action) {
            case "put":
            case "put_if_not_exists":
            case "number_add":
            case "number_subtract":
            case "list_append":
            case "list_prepend":
            case "list_set_at_index":
                return "SET";
            case "delete":
            case "list_delete_at_index":
                return "REMOVE";
            case "set_add":
                return "ADD";
            case "set_delete":
                return "DELETE";
        }
    }
}
