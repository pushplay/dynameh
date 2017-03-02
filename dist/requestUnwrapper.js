"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const responseUnwrapper_1 = require("./responseUnwrapper");
/**
 * Extract the item being put from the input.
 */
function unwrapPutInput(input) {
    return responseUnwrapper_1.unwrapResponseItem({ M: input.Item });
}
exports.unwrapPutInput = unwrapPutInput;
