export declare class BrevoTimeoutError extends Error {
    readonly cause?: unknown;
    constructor(message: string, opts?: {
        cause?: unknown;
    });
}
