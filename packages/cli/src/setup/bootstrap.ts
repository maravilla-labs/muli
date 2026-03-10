import * as dns from 'dns/promises';
import * as fs from 'fs';
import * as net from 'net';
import * as os from 'os';
import * as path from 'path';
import * as crypto from 'crypto';
import { execFileSync, spawnSync } from 'child_process';
import { createInterface } from 'readline/promises';
import { stdin as input, stdout as output } from 'process';
import chalk from 'chalk';
import { loadConfig, saveConfig, getConfigPath, type MuliConfig } from '../config.js';
import { buildClients, callRpc } from '../grpc.js';

export type SetupStateStatus = 'not-initialized' | 'partial' | 'initialized';
export type SecurityMode = 'dev' | 'secure-local';

export interface BootstrapState {
  profile: 'local-full';
  securityMode: SecurityMode;
  setupStatus: SetupStateStatus;
  onboardingComplete: boolean;
  tenantId: string;
  registryHost: string;
  gitHost: string;
  grpcPort: number;
  metricsPort: number;
  registryPort: number;
  gitPort: number;
  allowLocalhostWebhooks: boolean;
  tlsCertPath: string | null;
  tlsKeyPath: string | null;
  tlsCaCertPath: string | null;
  firstSetupAt: string | null;
  lastSetupAt: string | null;
  lastDoctorAt: string | null;
  completedSteps: Record<string, string>;
}

export interface StartSetupOptions {
  forceSetup: boolean;
  noSetup: boolean;
  localFullByDefault: boolean;
  requestedEmbeddedAgent?: boolean;
  userArgs: string[];
  preserveExistingPorts?: boolean;
}

export interface PreparedStart {
  extraArgs: string[];
  env: Record<string, string>;
  runOnboarding: boolean;
  didSetupRun: boolean;
}

export interface PreflightCheckResult {
  name: string;
  ok: boolean;
  message: string;
  remediation?: string;
}

const DEFAULT_TENANT = 'local';
const DEFAULT_REGISTRY_HOST = 'local.localhost';
const DEFAULT_GIT_HOST = 'local.localhost';
const DEFAULT_PORTS = {
  grpc: 50051,
  metrics: 9090,
  registry: 5000,
  git: 7000,
} as const;

function muliHome(): string {
  return process.env.MULI_HOME || path.join(os.homedir(), '.muli');
}

function runDir(): string {
  return path.join(muliHome(), 'run');
}

function certDir(): string {
  return path.join(muliHome(), 'certs');
}

function bootstrapPath(): string {
  return path.join(runDir(), 'bootstrap-state.json');
}

function ensureDirs(): void {
  fs.mkdirSync(runDir(), { recursive: true, mode: 0o755 });
  fs.mkdirSync(certDir(), { recursive: true, mode: 0o755 });
}

function defaultState(): BootstrapState {
  return {
    profile: 'local-full',
    securityMode: 'dev',
    setupStatus: 'not-initialized',
    onboardingComplete: false,
    tenantId: DEFAULT_TENANT,
    registryHost: DEFAULT_REGISTRY_HOST,
    gitHost: DEFAULT_GIT_HOST,
    grpcPort: DEFAULT_PORTS.grpc,
    metricsPort: DEFAULT_PORTS.metrics,
    registryPort: DEFAULT_PORTS.registry,
    gitPort: DEFAULT_PORTS.git,
    allowLocalhostWebhooks: false,
    tlsCertPath: null,
    tlsKeyPath: null,
    tlsCaCertPath: null,
    firstSetupAt: null,
    lastSetupAt: null,
    lastDoctorAt: null,
    completedSteps: {},
  };
}

export function loadBootstrapState(): BootstrapState {
  ensureDirs();
  const p = bootstrapPath();
  if (!fs.existsSync(p)) {
    return defaultState();
  }
  try {
    const parsed = JSON.parse(fs.readFileSync(p, 'utf8')) as Partial<BootstrapState>;
    return {
      ...defaultState(),
      ...parsed,
      completedSteps: parsed.completedSteps ?? {},
    };
  } catch {
    return defaultState();
  }
}

export function saveBootstrapState(state: BootstrapState): void {
  ensureDirs();
  fs.writeFileSync(bootstrapPath(), `${JSON.stringify(state, null, 2)}\n`, {
    encoding: 'utf8',
    mode: 0o600,
  });
}

export async function isPortAvailable(port: number): Promise<boolean> {
  return new Promise(resolve => {
    const server = net.createServer();
    server.once('error', () => resolve(false));
    server.once('listening', () => {
      server.close(() => resolve(true));
    });
    server.listen(port, '127.0.0.1');
  });
}

export async function resolveAvailablePort(preferred: number): Promise<number> {
  return resolveAvailablePortWithProbe(preferred, isPortAvailable);
}

export async function resolveAvailablePortWithProbe(
  preferred: number,
  probe: (port: number) => Promise<boolean>,
): Promise<number> {
  let candidate = preferred;
  for (let i = 0; i < 100; i += 1) {
    // eslint-disable-next-line no-await-in-loop
    if (await probe(candidate)) {
      return candidate;
    }
    candidate += 1;
  }
  throw new Error(`Could not find available port near ${preferred}`);
}

export function buildLocalFullArgs(ports: { registryPort: number; gitPort: number }, embeddedAgent: boolean, allowLocalhostWebhooks: boolean): string[] {
  const args = [
    '--registry',
    'full',
    '--git',
    '--default-tenant',
    DEFAULT_TENANT,
    '--registry-port',
    String(ports.registryPort),
    '--git-port',
    String(ports.gitPort),
  ];

  if (embeddedAgent) {
    args.push('--embedded-agent');
  }
  if (allowLocalhostWebhooks) {
    args.push('--allow-localhost-webhooks');
  }

  return args;
}

function hasOpenssl(): boolean {
  const result = spawnSync('openssl', ['version'], { stdio: 'ignore' });
  return result.status === 0;
}

function randomApiKey(): string {
  return crypto.randomBytes(24).toString('hex');
}

export function generateSecureLocalMaterials(): {
  apiKey: string;
  certPath: string;
  keyPath: string;
} {
  if (!hasOpenssl()) {
    throw new Error('openssl is required for secure-local setup but was not found in PATH');
  }

  ensureDirs();
  const certPath = path.join(certDir(), 'grpc-local-cert.pem');
  const keyPath = path.join(certDir(), 'grpc-local-key.pem');
  const apiKey = randomApiKey();

  execFileSync('openssl', [
    'req',
    '-x509',
    '-newkey',
    'rsa:2048',
    '-nodes',
    '-keyout',
    keyPath,
    '-out',
    certPath,
    '-days',
    '365',
    '-subj',
    '/CN=localhost',
    '-addext',
    'subjectAltName=DNS:localhost,IP:127.0.0.1',
  ], { stdio: 'ignore' });

  return { apiKey, certPath, keyPath };
}

export function mergeDockerDaemonJson(raw: string, registryAddr: string): string {
  const parsed = raw.trim() ? JSON.parse(raw) : {};
  const existing = Array.isArray(parsed['insecure-registries'])
    ? parsed['insecure-registries'] as string[]
    : [];

  if (!existing.includes(registryAddr)) {
    existing.push(registryAddr);
  }
  parsed['insecure-registries'] = existing;
  return `${JSON.stringify(parsed, null, 2)}\n`;
}

export function mergeNpmrc(raw: string, registryHost: string, registryPort: number, token: string): string {
  const lines = raw.split(/\r?\n/).filter(Boolean);
  const registryLine = `registry=http://${registryHost}:${registryPort}/-/npm/`;
  const tokenLine = `//${registryHost}:${registryPort}/-/npm/:_authToken=${token}`;

  const out = lines.filter(
    line => !line.startsWith('registry=') && !line.startsWith(`//${registryHost}:${registryPort}/-/npm/:_authToken=`),
  );
  out.push(registryLine, tokenLine);
  return `${out.join('\n')}\n`;
}

export function mergeCargoConfig(raw: string, registryHost: string, registryPort: number, token: string): string {
  const block = [
    '[registries.muli]',
    `index = "sparse+http://${registryHost}:${registryPort}/index/"`,
    `token = "${token}"`,
  ].join('\n');

  const trimmed = raw.trim();
  if (!trimmed) {
    return `${block}\n`;
  }

  if (trimmed.includes('[registries.muli]')) {
    const lines = raw.split(/\r?\n/);
    const out: string[] = [];
    let inBlock = false;
    for (const line of lines) {
      if (line.trim() === '[registries.muli]') {
        inBlock = true;
        continue;
      }
      if (inBlock && line.startsWith('[') && line.endsWith(']')) {
        inBlock = false;
      }
      if (!inBlock) {
        out.push(line);
      }
    }
    const next = out.join('\n').trim();
    return `${next ? `${next}\n\n` : ''}${block}\n`;
  }

  return `${trimmed}\n\n${block}\n`;
}

export function mergeMavenSettings(raw: string, token: string): string {
  const serverSnippet = [
    '  <server>',
    '    <id>muli</id>',
    '    <username>muli</username>',
    `    <password>${token}</password>`,
    '  </server>',
  ].join('\n');

  const repoHint = [
    '<!-- Add repository in your pom.xml or settings profile -->',
    '<!-- <repository><id>muli</id><url>http://local.localhost:5000/maven</url></repository> -->',
  ].join('\n');

  if (!raw.trim()) {
    return [
      '<settings xmlns="http://maven.apache.org/SETTINGS/1.0.0">',
      '  <servers>',
      serverSnippet,
      '  </servers>',
      `  ${repoHint}`,
      '</settings>',
      '',
    ].join('\n');
  }

  if (raw.includes('<id>muli</id>')) {
    return raw;
  }

  if (raw.includes('</servers>')) {
    return raw.replace('</servers>', `${serverSnippet}\n  </servers>`);
  }

  if (raw.includes('</settings>')) {
    return raw.replace('</settings>', `  <servers>\n${serverSnippet}\n  </servers>\n  ${repoHint}\n</settings>`);
  }

  return `${raw}\n\n${repoHint}\n`;
}

async function checkDockerDaemon(): Promise<PreflightCheckResult> {
  const res = spawnSync('docker', ['info'], { stdio: 'ignore' });
  if (res.status === 0) {
    return { name: 'docker', ok: true, message: 'Docker daemon reachable' };
  }
  return {
    name: 'docker',
    ok: false,
    message: 'Docker daemon is not reachable',
    remediation: 'Start Docker Desktop (macOS/Windows) or docker service (Linux).',
  };
}

async function checkGitBinary(): Promise<PreflightCheckResult> {
  const res = spawnSync('git', ['--version'], { stdio: 'ignore' });
  if (res.status === 0) {
    return { name: 'git', ok: true, message: 'git binary found' };
  }
  return {
    name: 'git',
    ok: false,
    message: 'git binary not found in PATH',
    remediation: 'Install git and retry `muli server start`.',
  };
}

async function checkLocalhostTenantResolution(): Promise<PreflightCheckResult> {
  try {
    const result = await dns.lookup('local.localhost');
    return {
      name: 'dns',
      ok: true,
      message: `local.localhost resolves (${result.address})`,
    };
  } catch {
    return {
      name: 'dns',
      ok: false,
      message: 'local.localhost does not resolve',
      remediation: 'Add to /etc/hosts: 127.0.0.1 local.localhost',
    };
  }
}

async function checkWritablePaths(): Promise<PreflightCheckResult> {
  try {
    ensureDirs();
    const configPath = getConfigPath();
    fs.mkdirSync(path.dirname(configPath), { recursive: true, mode: 0o700 });

    const fileA = path.join(runDir(), '.write-test');
    const fileB = path.join(path.dirname(configPath), '.write-test');

    fs.writeFileSync(fileA, 'ok', { mode: 0o600 });
    fs.writeFileSync(fileB, 'ok', { mode: 0o600 });
    fs.rmSync(fileA, { force: true });
    fs.rmSync(fileB, { force: true });

    return { name: 'paths', ok: true, message: 'CLI/server paths are writable' };
  } catch (err: any) {
    return {
      name: 'paths',
      ok: false,
      message: `Path write check failed: ${err.message}`,
      remediation: 'Ensure your user can write to ~/.muli and ~/.config/muli.',
    };
  }
}

export async function runDoctorChecks(): Promise<PreflightCheckResult[]> {
  const checks = await Promise.all([
    checkDockerDaemon(),
    checkGitBinary(),
    checkLocalhostTenantResolution(),
    checkWritablePaths(),
  ]);

  const state = loadBootstrapState();
  state.lastDoctorAt = new Date().toISOString();
  saveBootstrapState(state);

  return checks;
}

async function promptYesNo(message: string, defaultYes: boolean): Promise<boolean> {
  if (!process.stdin.isTTY || !process.stdout.isTTY) {
    return defaultYes;
  }

  const suffix = defaultYes ? '[Y/n]' : '[y/N]';
  const rl = createInterface({ input, output });
  try {
    const ans = (await rl.question(`${message} ${suffix} `)).trim().toLowerCase();
    if (!ans) return defaultYes;
    return ans === 'y' || ans === 'yes';
  } finally {
    rl.close();
  }
}

async function promptSecurityMode(defaultMode: SecurityMode): Promise<SecurityMode> {
  const secure = await promptYesNo('Enable secure-local mode (generate API key + self-signed TLS cert for gRPC)?', defaultMode === 'secure-local');
  return secure ? 'secure-local' : 'dev';
}

function nowIso(): string {
  return new Date().toISOString();
}

function updateCliConfigFromState(config: MuliConfig, state: BootstrapState, apiKey: string | null): MuliConfig {
  const protocol = state.securityMode === 'secure-local' ? 'https' : 'http';
  return {
    ...config,
    serverUrl: `${protocol}://localhost:${state.grpcPort}`,
    tenantId: state.tenantId,
    apiKey,
    tlsCaCertPath: state.securityMode === 'secure-local' ? state.tlsCaCertPath : null,
    registryHost: state.registryHost,
    registryPort: state.registryPort,
    gitHost: state.gitHost,
    gitPort: state.gitPort,
  };
}

function printChecks(checks: PreflightCheckResult[]): void {
  for (const check of checks) {
    if (check.ok) {
      console.log(chalk.green('✓'), `${check.name}: ${check.message}`);
    } else {
      console.log(chalk.red('✗'), `${check.name}: ${check.message}`);
      if (check.remediation) {
        console.log(chalk.yellow('  ->'), check.remediation);
      }
    }
  }
}

function failingChecks(checks: PreflightCheckResult[]): PreflightCheckResult[] {
  return checks.filter(c => !c.ok);
}

async function createBootstrapTokens(config: MuliConfig): Promise<{ registryToken: string; gitToken: string }> {
  const clients = buildClients(config);

  const registryRes = await callRpc<any>(
    clients.registry,
    'CreateRegistryToken',
    {
      tenant_id: config.tenantId,
      description: 'bootstrap-cli-local',
      ttl_seconds: 0,
      permissions: ['REGISTRY_PERMISSION_PULL', 'REGISTRY_PERMISSION_PUSH'],
    },
    clients.meta,
  );

  const gitRes = await callRpc<any>(
    clients.git,
    'CreateAccessToken',
    {
      tenant_id: config.tenantId,
      description: 'bootstrap-cli-local',
      permissions: ['GIT_PERMISSION_PULL', 'GIT_PERMISSION_PUSH'],
    },
    clients.meta,
  );

  return {
    registryToken: registryRes.plaintext_token ?? '',
    gitToken: gitRes.plaintext_token ?? '',
  };
}

async function waitForGrpc(config: MuliConfig, attempts = 40): Promise<void> {
  const clients = buildClients(config);
  let lastErr: Error | null = null;

  for (let i = 0; i < attempts; i += 1) {
    try {
      // eslint-disable-next-line no-await-in-loop
      await callRpc<any>(clients.health, 'Check', {}, clients.meta);
      return;
    } catch (err: any) {
      lastErr = err;
      // eslint-disable-next-line no-await-in-loop
      await new Promise(resolve => setTimeout(resolve, 250));
    }
  }

  throw new Error(`Timed out waiting for server health: ${lastErr?.message ?? 'unknown error'}`);
}

function previewWrite(filePath: string, content: string): void {
  console.log(chalk.dim(`\nPreview: ${filePath}`));
  const lines = content.split(/\r?\n/).slice(0, 20);
  for (const line of lines) {
    console.log(chalk.dim(`  ${line}`));
  }
  if (content.split(/\r?\n/).length > 20) {
    console.log(chalk.dim('  ...'));
  }
}

async function maybeWrite(filePath: string, next: string, label: string): Promise<boolean> {
  previewWrite(filePath, next);
  const confirm = await promptYesNo(`Apply ${label} changes?`, true);
  if (!confirm) {
    return false;
  }

  fs.mkdirSync(path.dirname(filePath), { recursive: true, mode: 0o755 });
  fs.writeFileSync(filePath, next, { encoding: 'utf8', mode: 0o600 });
  console.log(chalk.green('✓'), `Updated ${filePath}`);
  return true;
}

function readIfExists(filePath: string): string {
  if (!fs.existsSync(filePath)) {
    return '';
  }
  return fs.readFileSync(filePath, 'utf8');
}

async function runOnboardingWriters(config: MuliConfig, registryToken: string, gitToken: string): Promise<void> {
  const registryAddr = `${config.registryHost}:${config.registryPort}`;

  const dockerWanted = await promptYesNo('Configure Docker insecure registry for local HTTP registry?', true);
  if (dockerWanted) {
    const etcDaemon = '/etc/docker/daemon.json';
    const userDaemon = path.join(os.homedir(), '.docker', 'daemon.json');
    const target = fs.existsSync(etcDaemon) ? etcDaemon : userDaemon;
    try {
      const merged = mergeDockerDaemonJson(readIfExists(target), registryAddr);
      await maybeWrite(target, merged, 'Docker daemon.json');
      console.log(chalk.yellow('•'), 'Restart Docker daemon for changes to take effect.');
    } catch (err: any) {
      console.log(chalk.yellow('•'), `Could not patch Docker daemon config automatically: ${err.message}`);
      console.log(chalk.yellow('•'), `Add manually under insecure-registries: ${registryAddr}`);
    }
  }

  const npmWanted = await promptYesNo('Configure npm client (~/.npmrc) for local registry?', true);
  if (npmWanted) {
    const npmrcPath = path.join(os.homedir(), '.npmrc');
    const merged = mergeNpmrc(readIfExists(npmrcPath), config.registryHost, config.registryPort, registryToken);
    await maybeWrite(npmrcPath, merged, 'npm');
  }

  const cargoWanted = await promptYesNo('Configure Cargo client (~/.cargo/config.toml) for local registry?', false);
  if (cargoWanted) {
    const cargoPath = path.join(os.homedir(), '.cargo', 'config.toml');
    const merged = mergeCargoConfig(readIfExists(cargoPath), config.registryHost, config.registryPort, registryToken);
    await maybeWrite(cargoPath, merged, 'Cargo');
  }

  const mavenWanted = await promptYesNo('Configure Maven settings (~/.m2/settings.xml) with muli server credentials?', false);
  if (mavenWanted) {
    const mavenPath = path.join(os.homedir(), '.m2', 'settings.xml');
    const merged = mergeMavenSettings(readIfExists(mavenPath), registryToken);
    await maybeWrite(mavenPath, merged, 'Maven');
  }

  const gitWanted = await promptYesNo('Configure global git credential.helper=store?', false);
  if (gitWanted) {
    const res = spawnSync('git', ['config', '--global', 'credential.helper', 'store'], { stdio: 'ignore' });
    if (res.status === 0) {
      console.log(chalk.green('✓'), 'Configured git credential.helper=store');
    } else {
      console.log(chalk.yellow('•'), 'Failed to configure git credential helper automatically.');
    }
  }

  console.log(chalk.dim('\nGit usage hint:'));
  console.log(chalk.dim(`  git clone http://x-token:${gitToken}@${config.gitHost}:${config.gitPort}/<namespace>/<repo>`));
}

export async function runPostStartOnboarding(state: BootstrapState): Promise<void> {
  if (!process.stdin.isTTY || !process.stdout.isTTY) {
    return;
  }

  const shouldOnboard = await promptYesNo('Run first-run client onboarding now (Docker/npm/Cargo/Maven/Git)?', true);
  if (!shouldOnboard) {
    return;
  }

  const config = loadConfig();
  console.log(chalk.dim('Waiting for server readiness...'));
  await waitForGrpc(config);

  const createTokens = await promptYesNo('Create bootstrap registry/git access tokens for client setup?', true);
  if (!createTokens) {
    return;
  }

  const tokens = await createBootstrapTokens(config);
  if (!tokens.registryToken || !tokens.gitToken) {
    throw new Error('Server did not return bootstrap tokens');
  }

  console.log(chalk.green('✓'), 'Bootstrap tokens created (shown once):');
  console.log(`  Registry token: ${chalk.yellow(tokens.registryToken)}`);
  console.log(`  Git token:      ${chalk.yellow(tokens.gitToken)}`);

  await runOnboardingWriters(config, tokens.registryToken, tokens.gitToken);

  state.onboardingComplete = true;
  state.completedSteps.onboarding = nowIso();
  saveBootstrapState(state);
}

export async function prepareServerStart(options: StartSetupOptions): Promise<PreparedStart> {
  const state = loadBootstrapState();

  const runWithLocalFull = options.localFullByDefault && options.userArgs.length === 0;
  if (!runWithLocalFull) {
    return {
      extraArgs: options.userArgs,
      env: {},
      runOnboarding: false,
      didSetupRun: false,
    };
  }

  state.profile = 'local-full';

  const resolvedPorts = options.preserveExistingPorts
    ? {
      grpcPort: state.grpcPort || DEFAULT_PORTS.grpc,
      metricsPort: state.metricsPort || DEFAULT_PORTS.metrics,
      registryPort: state.registryPort || DEFAULT_PORTS.registry,
      gitPort: state.gitPort || DEFAULT_PORTS.git,
    }
    : {
      grpcPort: await resolveAvailablePort(state.grpcPort || DEFAULT_PORTS.grpc),
      metricsPort: await resolveAvailablePort(state.metricsPort || DEFAULT_PORTS.metrics),
      registryPort: await resolveAvailablePort(state.registryPort || DEFAULT_PORTS.registry),
      gitPort: await resolveAvailablePort(state.gitPort || DEFAULT_PORTS.git),
    };

  state.grpcPort = resolvedPorts.grpcPort;
  state.metricsPort = resolvedPorts.metricsPort;
  state.registryPort = resolvedPorts.registryPort;
  state.gitPort = resolvedPorts.gitPort;
  state.tenantId = DEFAULT_TENANT;
  state.registryHost = DEFAULT_REGISTRY_HOST;
  state.gitHost = DEFAULT_GIT_HOST;

  let setupRan = false;
  let apiKeyForConfig: string | null = null;
  const shouldRunSetup = !options.noSetup && (options.forceSetup || state.setupStatus !== 'initialized');

  if (shouldRunSetup) {
    setupRan = true;
    const checks = await runDoctorChecks();
    printChecks(checks);

    const failing = failingChecks(checks);
    if (failing.length > 0) {
      state.setupStatus = 'partial';
      state.lastSetupAt = nowIso();
      saveBootstrapState(state);
      throw new Error('Preflight checks failed. Resolve the issues above or run `muli setup doctor`.');
    }

    state.securityMode = await promptSecurityMode(state.securityMode);
    state.allowLocalhostWebhooks = await promptYesNo('Allow localhost/private-IP webhooks (dev only)?', false);

    if (state.securityMode === 'secure-local') {
      const materials = generateSecureLocalMaterials();
      apiKeyForConfig = materials.apiKey;
      state.tlsCertPath = materials.certPath;
      state.tlsKeyPath = materials.keyPath;
      state.tlsCaCertPath = materials.certPath;
      state.completedSteps.security = nowIso();
    } else {
      state.tlsCertPath = null;
      state.tlsKeyPath = null;
      state.tlsCaCertPath = null;
    }

    state.setupStatus = 'initialized';
    state.firstSetupAt = state.firstSetupAt ?? nowIso();
    state.lastSetupAt = nowIso();
    state.completedSteps.preflight = nowIso();
    state.completedSteps.profile = nowIso();
    saveBootstrapState(state);
  }

  const config = loadConfig();
  let effectiveApiKey = apiKeyForConfig ?? (state.securityMode === 'secure-local' ? config.apiKey : null);
  if (state.securityMode === 'secure-local' && !effectiveApiKey) {
    effectiveApiKey = randomApiKey();
  }

  const nextCfg = updateCliConfigFromState(config, state, effectiveApiKey);
  saveConfig(nextCfg);

  const embeddedAgent = options.requestedEmbeddedAgent ?? true;
  const args = buildLocalFullArgs(
    { registryPort: state.registryPort, gitPort: state.gitPort },
    embeddedAgent,
    state.allowLocalhostWebhooks,
  );

  const env: Record<string, string> = {
    MULI_GRPC_PORT: String(state.grpcPort),
    MULI_METRICS_PORT: String(state.metricsPort),
    MULI_REGISTRY_DOMAIN: 'localhost',
    MULI_GIT_DOMAIN: 'localhost',
  };

  if (state.securityMode === 'secure-local') {
    env.MULI_REQUIRE_AUTH = 'true';
    env.MULI_API_KEY = nextCfg.apiKey ?? '';
    if (state.tlsCertPath && state.tlsKeyPath) {
      env.MULI_TLS_CERT_PATH = state.tlsCertPath;
      env.MULI_TLS_KEY_PATH = state.tlsKeyPath;
    }
  }

  return {
    extraArgs: args,
    env,
    runOnboarding: setupRan && !state.onboardingComplete,
    didSetupRun: setupRan,
  };
}
