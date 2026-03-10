import * as grpc from '@grpc/grpc-js';
import type { MuliConfig } from './config.js';
export interface MuliClients {
    health: any;
    git: any;
    registry: any;
    job: any;
    log: any;
    meta: grpc.Metadata;
}
export declare function buildClients(config: MuliConfig): MuliClients;
export declare function callRpc<T>(client: any, method: string, request: any, meta: grpc.Metadata): Promise<T>;
export declare function streamRpc(client: any, method: string, request: any, meta: grpc.Metadata): grpc.ClientReadableStream<any>;
//# sourceMappingURL=grpc.d.ts.map