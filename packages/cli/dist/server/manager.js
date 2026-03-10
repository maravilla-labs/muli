import * as crypto from 'crypto';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { spawn } from 'child_process';
const RELEASE_OWNER = 'maravilla-labs';
const RELEASE_REPO = 'muli';
function dataDir() {
    if (process.env.MULI_HOME) {
        return process.env.MULI_HOME;
    }
    return path.join(os.homedir(), '.muli');
}
function binDir() {
    return path.join(dataDir(), 'bin');
}
function runDir() {
    return path.join(dataDir(), 'run');
}
function statePath() {
    return path.join(runDir(), 'server-state.json');
}
function logPath() {
    return path.join(runDir(), 'server.log');
}
function ensureDirs() {
    fs.mkdirSync(binDir(), { recursive: true, mode: 0o755 });
    fs.mkdirSync(runDir(), { recursive: true, mode: 0o755 });
}
function defaultState() {
    return {
        installedVersion: null,
        binaryPath: null,
        pid: null,
        startedAt: null,
        managedArgs: [],
        managedEmbeddedAgent: false,
        managedDetached: true,
        lastCheckedAt: null,
        lastKnownLatestVersion: null,
    };
}
export function loadState() {
    ensureDirs();
    const p = statePath();
    if (!fs.existsSync(p)) {
        return defaultState();
    }
    try {
        const parsed = JSON.parse(fs.readFileSync(p, 'utf8'));
        return {
            ...defaultState(),
            ...parsed,
            managedArgs: Array.isArray(parsed.managedArgs) ? parsed.managedArgs : [],
        };
    }
    catch {
        return defaultState();
    }
}
export function saveState(state) {
    ensureDirs();
    fs.writeFileSync(statePath(), JSON.stringify(state, null, 2) + '\n', {
        encoding: 'utf8',
        mode: 0o600,
    });
}
export function normalizeVersion(value) {
    const v = value.trim().replace(/^v/, '');
    if (!/^\d+\.\d+\.\d+([.-][0-9A-Za-z.-]+)?$/.test(v)) {
        throw new Error(`Invalid version: ${value}`);
    }
    return v;
}
export function compareVersions(aRaw, bRaw) {
    const a = normalizeVersion(aRaw).split('-')[0].split('.').map(Number);
    const b = normalizeVersion(bRaw).split('-')[0].split('.').map(Number);
    for (let i = 0; i < 3; i += 1) {
        if (a[i] > b[i])
            return 1;
        if (a[i] < b[i])
            return -1;
    }
    return 0;
}
function toTag(version) {
    return `v${normalizeVersion(version)}`;
}
function fromTag(tag) {
    return normalizeVersion(tag);
}
function requestHeaders() {
    const headers = {
        'User-Agent': 'muli-cli',
        Accept: 'application/vnd.github+json',
    };
    if (process.env.GITHUB_TOKEN) {
        headers.Authorization = `Bearer ${process.env.GITHUB_TOKEN}`;
    }
    return headers;
}
function releaseApiBase() {
    return `https://api.github.com/repos/${RELEASE_OWNER}/${RELEASE_REPO}/releases`;
}
async function fetchRelease(url) {
    const res = await fetch(url, { headers: requestHeaders() });
    if (!res.ok) {
        throw new Error(`GitHub API request failed (${res.status})`);
    }
    return (await res.json());
}
export async function fetchLatestRelease() {
    return fetchRelease(`${releaseApiBase()}/latest`);
}
export async function fetchReleaseByVersion(version) {
    const tag = toTag(version);
    return fetchRelease(`${releaseApiBase()}/tags/${encodeURIComponent(tag)}`);
}
export function resolveTarget() {
    const platform = process.platform;
    const arch = process.arch;
    if (platform === 'darwin' && arch === 'x64') {
        return { target: 'darwin-x86_64', ext: '' };
    }
    if (platform === 'darwin' && arch === 'arm64') {
        return { target: 'darwin-aarch64', ext: '' };
    }
    if (platform === 'linux' && arch === 'x64') {
        return { target: 'linux-x86_64', ext: '' };
    }
    if (platform === 'win32' && arch === 'x64') {
        return { target: 'windows-x86_64', ext: '.exe' };
    }
    throw new Error(`Unsupported platform for auto-download: ${platform}/${arch}. Supported: darwin (x64, arm64), linux (x64), windows (x64)`);
}
export function serverAssetName(version, target, ext) {
    return `muli-server-${normalizeVersion(version)}-${target}${ext}`;
}
export function checksumsAssetName(version) {
    return `checksums-${normalizeVersion(version)}.txt`;
}
function binaryInstallPath(ext) {
    return path.join(binDir(), `muli-server${ext}`);
}
function findAsset(release, name) {
    const asset = release.assets.find(a => a.name === name);
    if (!asset) {
        throw new Error(`Release asset not found: ${name}`);
    }
    return asset;
}
async function downloadToFile(url, destPath) {
    const res = await fetch(url, { headers: requestHeaders() });
    if (!res.ok) {
        throw new Error(`Download failed (${res.status}) for ${url}`);
    }
    const arrayBuffer = await res.arrayBuffer();
    fs.writeFileSync(destPath, Buffer.from(arrayBuffer));
}
function sha256File(filePath) {
    const hash = crypto.createHash('sha256');
    const data = fs.readFileSync(filePath);
    hash.update(data);
    return hash.digest('hex');
}
function checksumFromFile(checksumsPath, assetName) {
    const raw = fs.readFileSync(checksumsPath, 'utf8');
    const lines = raw.split(/\r?\n/).map(v => v.trim()).filter(Boolean);
    for (const line of lines) {
        const parts = line.split(/\s+/);
        if (parts.length < 2)
            continue;
        const hash = parts[0];
        const fileName = parts[parts.length - 1].replace(/^\*/, '');
        if (fileName === assetName) {
            return hash;
        }
    }
    return null;
}
function isProcessRunning(pid) {
    try {
        process.kill(pid, 0);
        return true;
    }
    catch {
        return false;
    }
}
function setExecutable(filePath) {
    if (process.platform !== 'win32') {
        fs.chmodSync(filePath, 0o755);
    }
}
export async function installServer(version, force = false) {
    ensureDirs();
    const release = version
        ? await fetchReleaseByVersion(version)
        : await fetchLatestRelease();
    const resolvedVersion = fromTag(release.tag_name);
    const { target, ext } = resolveTarget();
    const binaryName = serverAssetName(resolvedVersion, target, ext);
    const checksumName = checksumsAssetName(resolvedVersion);
    const binaryAsset = findAsset(release, binaryName);
    const checksumAsset = findAsset(release, checksumName);
    const state = loadState();
    const installPath = binaryInstallPath(ext);
    if (!force && state.installedVersion === resolvedVersion && fs.existsSync(installPath)) {
        return {
            version: resolvedVersion,
            binaryPath: installPath,
            changed: false,
        };
    }
    const tmpBinary = `${installPath}.tmp-${Date.now()}`;
    const tmpChecksum = path.join(runDir(), `${checksumName}.tmp-${Date.now()}`);
    await downloadToFile(binaryAsset.browser_download_url, tmpBinary);
    await downloadToFile(checksumAsset.browser_download_url, tmpChecksum);
    const expected = checksumFromFile(tmpChecksum, binaryName);
    if (!expected) {
        throw new Error(`Checksum entry for ${binaryName} not found in ${checksumName}`);
    }
    const actual = sha256File(tmpBinary);
    if (actual !== expected) {
        throw new Error('Checksum verification failed for downloaded server binary');
    }
    setExecutable(tmpBinary);
    fs.renameSync(tmpBinary, installPath);
    try {
        fs.rmSync(tmpChecksum, { force: true });
    }
    catch {
        // ignore cleanup failures
    }
    state.installedVersion = resolvedVersion;
    state.binaryPath = installPath;
    saveState(state);
    return {
        version: resolvedVersion,
        binaryPath: installPath,
        changed: true,
    };
}
export async function stopManagedServer(force = false) {
    const state = loadState();
    const pid = state.pid;
    if (!pid || !isProcessRunning(pid)) {
        state.pid = null;
        state.startedAt = null;
        saveState(state);
        return { stopped: false, pid: null };
    }
    try {
        process.kill(pid, 'SIGTERM');
    }
    catch {
        if (force) {
            process.kill(pid, 'SIGKILL');
        }
        else {
            throw new Error(`Failed to stop process ${pid}`);
        }
    }
    const deadline = Date.now() + 8000;
    while (Date.now() < deadline) {
        if (!isProcessRunning(pid)) {
            state.pid = null;
            state.startedAt = null;
            saveState(state);
            return { stopped: true, pid };
        }
        await new Promise(resolve => setTimeout(resolve, 250));
    }
    if (force) {
        process.kill(pid, 'SIGKILL');
        state.pid = null;
        state.startedAt = null;
        saveState(state);
        return { stopped: true, pid };
    }
    throw new Error(`Process ${pid} did not stop within timeout`);
}
async function ensureInstalled(version, forceInstall = false) {
    const state = loadState();
    if (!version
        && state.installedVersion
        && state.binaryPath
        && fs.existsSync(state.binaryPath)
        && !forceInstall) {
        return {
            version: state.installedVersion,
            binaryPath: state.binaryPath,
            changed: false,
        };
    }
    return installServer(version, forceInstall);
}
export async function startServer(options) {
    const install = await ensureInstalled(options.version);
    const state = loadState();
    if (state.pid && isProcessRunning(state.pid)) {
        if (!options.force) {
            throw new Error(`Server already running with PID ${state.pid}. Stop it first or use --force.`);
        }
        await stopManagedServer(true);
    }
    const args = [];
    if (options.embeddedAgent) {
        args.push('--embedded-agent');
    }
    args.push(...options.extraArgs);
    if (options.detach) {
        const out = fs.openSync(logPath(), 'a');
        const child = spawn(install.binaryPath, args, {
            detached: true,
            env: { ...process.env, ...(options.env ?? {}) },
            stdio: ['ignore', out, out],
        });
        child.unref();
        state.pid = child.pid ?? null;
        state.startedAt = new Date().toISOString();
        state.managedArgs = options.extraArgs;
        state.managedEmbeddedAgent = !!options.embeddedAgent;
        state.managedDetached = true;
        state.installedVersion = install.version;
        state.binaryPath = install.binaryPath;
        saveState(state);
        return {
            pid: child.pid ?? null,
            version: install.version,
            binaryPath: install.binaryPath,
        };
    }
    state.managedArgs = options.extraArgs;
    state.managedEmbeddedAgent = !!options.embeddedAgent;
    state.managedDetached = false;
    state.installedVersion = install.version;
    state.binaryPath = install.binaryPath;
    saveState(state);
    const child = spawn(install.binaryPath, args, {
        env: { ...process.env, ...(options.env ?? {}) },
        stdio: 'inherit',
    });
    await new Promise((resolve, reject) => {
        child.on('exit', () => resolve());
        child.on('error', reject);
    });
    return {
        pid: null,
        version: install.version,
        binaryPath: install.binaryPath,
    };
}
export async function getServerStatus() {
    const { loadBootstrapState } = await import('../setup/bootstrap.js');
    const state = loadState();
    const bootstrap = loadBootstrapState();
    const running = !!state.pid && isProcessRunning(state.pid);
    if (!running && state.pid) {
        state.pid = null;
        state.startedAt = null;
        saveState(state);
    }
    let latestVersion = null;
    let updateState = 'latest-unknown';
    try {
        const latest = await fetchLatestRelease();
        latestVersion = fromTag(latest.tag_name);
        state.lastKnownLatestVersion = latestVersion;
        state.lastCheckedAt = new Date().toISOString();
        saveState(state);
        if (!state.installedVersion) {
            updateState = 'update-available';
        }
        else {
            updateState = compareVersions(state.installedVersion, latestVersion) < 0
                ? 'update-available'
                : 'up-to-date';
        }
    }
    catch {
        latestVersion = state.lastKnownLatestVersion;
        updateState = 'latest-unknown';
    }
    return {
        installedVersion: state.installedVersion,
        binaryPath: state.binaryPath,
        running,
        pid: running ? state.pid : null,
        startedAt: running ? state.startedAt : null,
        latestVersion,
        updateState,
        setupStatus: bootstrap.setupStatus,
    };
}
export async function updateServer(options) {
    const state = loadState();
    const running = !!state.pid && isProcessRunning(state.pid);
    let targetVersion = null;
    if (options.toVersion) {
        targetVersion = normalizeVersion(options.toVersion);
    }
    else {
        try {
            const latest = await fetchLatestRelease();
            targetVersion = fromTag(latest.tag_name);
        }
        catch {
            if (options.check) {
                return {
                    action: 'checked',
                    installedVersion: state.installedVersion,
                    targetVersion: null,
                    updateState: 'latest-unknown',
                    restarted: false,
                };
            }
            throw new Error('Could not determine latest release version');
        }
    }
    const installedVersion = state.installedVersion;
    let updateState;
    if (!installedVersion) {
        updateState = 'update-available';
    }
    else {
        updateState = compareVersions(installedVersion, targetVersion) < 0
            ? 'update-available'
            : 'up-to-date';
    }
    if (options.check) {
        return {
            action: 'checked',
            installedVersion,
            targetVersion,
            updateState,
            restarted: false,
        };
    }
    const needsUpdate = !installedVersion || compareVersions(installedVersion, targetVersion) < 0;
    if (!needsUpdate && !options.force) {
        return {
            action: 'noop',
            installedVersion,
            targetVersion,
            updateState: 'up-to-date',
            restarted: false,
        };
    }
    if (running && !options.force) {
        throw new Error('Server is running. Stop it first or run update with --force.');
    }
    let shouldRestart = false;
    if (running && options.force) {
        await stopManagedServer(true);
        shouldRestart = true;
    }
    const install = await installServer(targetVersion, true);
    if (shouldRestart) {
        await startServer({
            version: install.version,
            embeddedAgent: state.managedEmbeddedAgent,
            detach: state.managedDetached,
            extraArgs: state.managedArgs,
            force: true,
        });
    }
    return {
        action: 'updated',
        installedVersion: install.version,
        targetVersion,
        updateState: 'up-to-date',
        restarted: shouldRestart,
    };
}
//# sourceMappingURL=manager.js.map