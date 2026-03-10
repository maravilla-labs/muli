import React from 'react';
import type { MuliClients } from '../grpc.js';
interface LogStreamProps {
    jobId: string;
    clients: MuliClients;
    follow?: boolean;
    onDone?: (exitCode: number) => void;
}
export declare const LogStream: React.FC<LogStreamProps>;
export {};
//# sourceMappingURL=log-stream.d.ts.map