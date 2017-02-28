"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : new P(function (resolve) { resolve(result.value); }).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
/**
 * The maximum number of concurrent requests to have pending.
 */
exports.concurrentFactor = 20;
/**
 * Manages a number of concurrent dynamodb putItem requests.  Put requests
 * are more powerful than batchWrites but cannot be done in batch form.  Making
 * them concurrently is the next best thing.
 * @param dynamodb
 * @param putInputs
 * @returns a Promise of an array of objects of errors with the corresponding input
 */
function concurrentPutAll(dynamodb, putInputs) {
    return __awaiter(this, void 0, void 0, function* () {
        const putThunks = putInputs.map(putInput => () => __awaiter(this, void 0, void 0, function* () { return dynamodb.putItem(putInput).promise(); }));
        const results = yield runConcurrentThunks(putThunks);
        return results
            .map((maybeError, ix) => {
            if (maybeError instanceof Error) {
                return {
                    input: putInputs[ix],
                    error: maybeError
                };
            }
            return null;
        })
            .filter(o => !!o);
    });
}
exports.concurrentPutAll = concurrentPutAll;
/**
 * Executes thunks concurrently and returns a Promise of the
 * result or an Error object.
 */
function runConcurrentThunks(thunks) {
    return new Promise(resolve => {
        const results = [];
        let startedCount = 0;
        let resultCount = 0;
        function onDone(res, ix) {
            results[ix] = res;
            resultCount++;
            if (resultCount === thunks.length) {
                resolve(results);
            }
            else if (startedCount < thunks.length) {
                startNext();
            }
        }
        function startNext() {
            return __awaiter(this, void 0, void 0, function* () {
                const ix = startedCount++;
                try {
                    const p = thunks[ix]();
                    const result = yield p;
                    onDone(result, ix);
                }
                catch (err) {
                    onDone(err, ix);
                }
            });
        }
        for (let i = 0; i < exports.concurrentFactor && i < thunks.length; i++) {
            startNext();
        }
    });
}
