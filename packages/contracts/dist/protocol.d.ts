import { type ApplyPlanInput, type CoreMethodMap, type CoreMethodName, type CoreProtocolVersion, type ProgressEvent } from "./dto.js";
import { type CoreErrorCode, type CoreErrorDto, type CoreErrorSeverity } from "./errors.js";
export interface CoreRequestEnvelope<M extends CoreMethodName = CoreMethodName> {
    protocolVersion: CoreProtocolVersion;
    requestId: string;
    operationId?: string;
    method: M;
    payload: CoreMethodMap[M]["input"];
}
export type CoreResponseEnvelope<M extends CoreMethodName = CoreMethodName> = {
    protocolVersion: CoreProtocolVersion;
    requestId: string;
    operationId?: string;
    ok: true;
    result: CoreMethodMap[M]["output"];
} | {
    protocolVersion: CoreProtocolVersion;
    requestId: string;
    operationId?: string;
    ok: false;
    error: CoreErrorDto;
};
export interface CoreProgressEnvelope {
    protocolVersion: CoreProtocolVersion;
    requestId: string;
    operationId: string;
    event: "progress";
    progress: ProgressEvent;
}
export declare class ContractValidationError extends Error {
    readonly code: "INVALID_INPUT" | "PROTOCOL_VERSION_MISMATCH";
    constructor(code: "INVALID_INPUT" | "PROTOCOL_VERSION_MISMATCH", message: string);
}
export declare function assertProtocolVersion(value: unknown): asserts value is CoreProtocolVersion;
export declare function assertApplyPlanInput(value: unknown): asserts value is ApplyPlanInput;
export declare function assertCoreMethodInput<M extends CoreMethodName>(method: M, value: unknown): asserts value is CoreMethodMap[M]["input"];
export declare function assertCoreErrorDto(value: unknown): asserts value is CoreErrorDto;
export declare function assertCoreRequestEnvelope<M extends CoreMethodName = CoreMethodName>(value: unknown): asserts value is CoreRequestEnvelope<M>;
export declare function assertCoreResponseEnvelope<M extends CoreMethodName = CoreMethodName>(value: unknown, expectedRequestId?: string): asserts value is CoreResponseEnvelope<M>;
export declare function assertCoreMethodOutput<M extends CoreMethodName>(method: M, value: unknown): asserts value is CoreMethodMap[M]["output"];
export declare function assertProgressEvent(value: unknown): asserts value is ProgressEvent;
export declare function createCoreRequestEnvelope<M extends CoreMethodName>(method: M, payload: CoreMethodMap[M]["input"], requestId: string, operationId?: string): CoreRequestEnvelope<M>;
export declare function createCoreSuccessEnvelope<M extends CoreMethodName>(request: CoreRequestEnvelope<M>, result: CoreMethodMap[M]["output"], operationId?: string): CoreResponseEnvelope<M>;
export declare function createCoreFailureEnvelope<M extends CoreMethodName>(request: CoreRequestEnvelope<M>, error: CoreErrorDto): CoreResponseEnvelope<M>;
export declare function isCoreErrorCode(value: unknown): value is CoreErrorCode;
export declare function isCoreErrorSeverity(value: unknown): value is CoreErrorSeverity;
