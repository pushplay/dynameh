import * as aws from "aws-sdk";

/**
 * The maximum number of concurrent requests to have pending.
 */
export let concurrentFactor = 20;

/**
 * The initial wait when backing off on request rate.
 */
export let backoffInitial = 2000;

/**
 * The maximum wait when backing off on request rate.
 */
export let backoffMax = 60000;

/**
 * The wait growth factor when repeatedly backing off.
 */
export let backoffFactor = 2.0;

/**
 * Manages a number of concurrent dynamodb putItem requests.  Put requests
 * are more powerful than batchWrites but cannot be done in batch form.  Making
 * them concurrently is the next best thing.
 * @param dynamodb
 * @param putInputs
 * @returns A Promise of an array of objects of errors with the corresponding input.
 */
export async function concurrentPutAll(dynamodb: aws.DynamoDB, putInputs: aws.DynamoDB.PutItemInput[]): Promise<{ input: aws.DynamoDB.PutItemInput, error: Error }[]> {
    const putThunks = putInputs.map(putInput => async () => dynamodb.putItem(putInput).promise());
    const results = await runConcurrentThunks(putThunks);
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
}

type ThunkPromise<T> = () => Promise<T>;

/**
 * Executes thunks concurrently and returns a Promise of the
 * result or an Error object.
 */
function runConcurrentThunks<T>(thunks: ThunkPromise<T>[]): Promise<(T | Error)[]> {
    return new Promise(resolve => {
        const results: T[] = [];
        let startedCount = 0;
        let resultCount = 0;

        function onDone(res: T, ix: number): void {
            results[ix] = res;
            resultCount++;
            if (resultCount === thunks.length) {
                resolve(results);
            } else if (startedCount < thunks.length) {
                startNext();
            }
        }

        async function startNext(): Promise<void> {
            const ix = startedCount++;
            let backoff = backoffInitial;
            while (true) {
                try {
                    const p = thunks[ix]();
                    const result = await p;
                    onDone(result, ix);
                    return;
                } catch (err) {
                    if (!err.retryable) {
                        onDone(err, ix);
                        return;
                    }
                    await new Promise(resolve => setTimeout(resolve, backoff));
                    backoff = Math.min(backoffFactor * backoff, backoffMax);
                }
            }
        }

        for (let i = 0; i < concurrentFactor && i < thunks.length; i++) {
            startNext();
        }
    });
}
