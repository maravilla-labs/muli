import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import * as fs from 'fs';
import * as path from 'path';
import { fileURLToPath } from 'url';
// ESM-compatible __dirname
const _filename = fileURLToPath(import.meta.url);
const _dirname = path.dirname(_filename);
const PROTO_OPTIONS = {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
};
function resolveProtoBase() {
    if (process.env.MULI_PROTO_PATH) {
        return process.env.MULI_PROTO_PATH;
    }
    const packagedProto = path.resolve(_dirname, '..', 'proto');
    if (fs.existsSync(packagedProto)) {
        return packagedProto;
    }
    // Repo-local fallback for development (running from source without build copy).
    return path.resolve(_dirname, '..', '..', '..', 'proto');
}
function resolveProtoFiles(baseDir) {
    return [
        'muli/v1/health.proto',
        'muli/v1/git.proto',
        'muli/v1/registry.proto',
        'muli/v1/job.proto',
        'muli/v1/log.proto',
        'muli/v1/common.proto',
    ].map(f => path.join(baseDir, f));
}
export function buildClients(config) {
    const parsed = parseServerUrl(config.serverUrl);
    const baseDir = resolveProtoBase();
    const protoFiles = resolveProtoFiles(baseDir);
    const pkgDef = protoLoader.loadSync(protoFiles, {
        ...PROTO_OPTIONS,
        includeDirs: [baseDir],
    });
    const pkg = grpc.loadPackageDefinition(pkgDef);
    const creds = buildChannelCredentials(parsed, config);
    const meta = new grpc.Metadata();
    if (config.apiKey) {
        meta.set('authorization', `Bearer ${config.apiKey}`);
    }
    meta.set('x-tenant-id', config.tenantId);
    const addr = parsed.host;
    return {
        health: new pkg.muli.v1.HealthService(addr, creds),
        git: new pkg.muli.v1.GitService(addr, creds),
        registry: new pkg.muli.v1.RegistryService(addr, creds),
        job: new pkg.muli.v1.JobService(addr, creds),
        log: new pkg.muli.v1.LogService(addr, creds),
        meta,
    };
}
function parseServerUrl(serverUrl) {
    try {
        if (/^https?:\/\//.test(serverUrl)) {
            return new URL(serverUrl);
        }
        return new URL(`http://${serverUrl}`);
    }
    catch (err) {
        throw new Error(`Invalid server URL: ${serverUrl} (${err.message})`);
    }
}
function buildChannelCredentials(parsed, config) {
    if (parsed.protocol === 'https:') {
        const caPath = config.tlsCaCertPath ?? process.env.MULI_TLS_CA_CERT_PATH;
        if (caPath) {
            const caCert = fs.readFileSync(caPath);
            return grpc.credentials.createSsl(caCert);
        }
        return grpc.credentials.createSsl();
    }
    return grpc.credentials.createInsecure();
}
export function callRpc(client, method, request, meta) {
    return new Promise((resolve, reject) => {
        client[method](request, meta, (err, response) => {
            if (err)
                reject(err);
            else
                resolve(response);
        });
    });
}
export function streamRpc(client, method, request, meta) {
    return client[method](request, meta);
}
//# sourceMappingURL=grpc.js.map