export type UpdateState = 'up-to-date' | 'update-available' | 'latest-unknown';
export interface ServerState {
    installedVersion: string | null;
    binaryPath: string | null;
    pid: number | null;
    startedAt: string | null;
    managedArgs: string[];
    managedEmbeddedAgent: boolean;
    managedDetached: boolean;
    lastCheckedAt: string | null;
    lastKnownLatestVersion: string | null;
}
interface ReleaseAsset {
    name: string;
    browser_download_url: string;
}
interface GithubRelease {
    tag_name: string;
    assets: ReleaseAsset[];
}
export interface InstallResult {
    version: string;
    binaryPath: string;
    changed: boolean;
}
export interface StatusResult {
    installedVersion: string | null;
    binaryPath: string | null;
    running: boolean;
    pid: number | null;
    startedAt: string | null;
    latestVersion: string | null;
    updateState: UpdateState;
    setupStatus: 'initialized' | 'partial' | 'not-initialized';
}
export interface StartOptions {
    version?: string;
    embeddedAgent?: boolean;
    detach: boolean;
    extraArgs: string[];
    force?: boolean;
    env?: Record<string, string>;
}
export interface UpdateOptions {
    toVersion?: string;
    check?: boolean;
    force?: boolean;
}
export declare function loadState(): ServerState;
export declare function saveState(state: ServerState): void;
export declare function normalizeVersion(value: string): string;
export declare function compareVersions(aRaw: string, bRaw: string): number;
export declare function fetchLatestRelease(): Promise<GithubRelease>;
export declare function fetchReleaseByVersion(version: string): Promise<GithubRelease>;
export declare function resolveTarget(): {
    target: string;
    ext: string;
};
export declare function serverAssetName(version: string, target: string, ext: string): string;
export declare function checksumsAssetName(version: string): string;
export declare function installServer(version?: string, force?: boolean): Promise<InstallResult>;
export declare function stopManagedServer(force?: boolean): Promise<{
    stopped: boolean;
    pid: number | null;
}>;
export declare function startServer(options: StartOptions): Promise<{
    pid: number | null;
    version: string;
    binaryPath: string;
}>;
export declare function getServerStatus(): Promise<StatusResult>;
export declare function updateServer(options: UpdateOptions): Promise<{
    action: 'checked' | 'updated' | 'noop';
    installedVersion: string | null;
    targetVersion: string | null;
    updateState: UpdateState;
    restarted: boolean;
}>;
export {};
//# sourceMappingURL=manager.d.ts.map