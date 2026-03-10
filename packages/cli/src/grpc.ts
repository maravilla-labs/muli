import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import * as fs from 'fs';
import * as path from 'path';
import { fileURLToPath } from 'url';
import type { MuliConfig } from './config.js';

// ESM-compatible __dirname
const _filename = fileURLToPath(import.meta.url);
const _dirname = path.dirname(_filename);

const PROTO_OPTIONS: protoLoader.Options = {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
};

function resolveProtoBase(): string {
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

function resolveProtoFiles(baseDir: string): string[] {
  return [
    'muli/v1/health.proto',
    'muli/v1/git.proto',
    'muli/v1/registry.proto',
    'muli/v1/job.proto',
    'muli/v1/log.proto',
    'muli/v1/common.proto',
  ].map(f => path.join(baseDir, f));
}

export interface MuliClients {
  health: any;
  git: any;
  registry: any;
  job: any;
  log: any;
  meta: grpc.Metadata;
}

export function buildClients(config: MuliConfig): MuliClients {
  const parsed = parseServerUrl(config.serverUrl);
  const baseDir = resolveProtoBase();
  const protoFiles = resolveProtoFiles(baseDir);

  const pkgDef = protoLoader.loadSync(protoFiles, {
    ...PROTO_OPTIONS,
    includeDirs: [baseDir],
  });

  const pkg = grpc.loadPackageDefinition(pkgDef) as any;
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

function parseServerUrl(serverUrl: string): URL {
  try {
    if (/^https?:\/\//.test(serverUrl)) {
      return new URL(serverUrl);
    }
    return new URL(`http://${serverUrl}`);
  } catch (err: any) {
    throw new Error(`Invalid server URL: ${serverUrl} (${err.message})`);
  }
}

function buildChannelCredentials(
  parsed: URL,
  config: MuliConfig,
): grpc.ChannelCredentials {
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

export function callRpc<T>(
  client: any,
  method: string,
  request: any,
  meta: grpc.Metadata,
): Promise<T> {
  return new Promise((resolve, reject) => {
    client[method](
      request,
      meta,
      (err: grpc.ServiceError | null, response: T) => {
        if (err) reject(err);
        else resolve(response);
      },
    );
  });
}

export function streamRpc(
  client: any,
  method: string,
  request: any,
  meta: grpc.Metadata,
): grpc.ClientReadableStream<any> {
  return client[method](request, meta);
}
