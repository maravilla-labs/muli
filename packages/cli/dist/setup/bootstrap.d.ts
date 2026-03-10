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
export declare function loadBootstrapState(): BootstrapState;
export declare function saveBootstrapState(state: BootstrapState): void;
export declare function isPortAvailable(port: number): Promise<boolean>;
export declare function resolveAvailablePort(preferred: number): Promise<number>;
export declare function resolveAvailablePortWithProbe(preferred: number, probe: (port: number) => Promise<boolean>): Promise<number>;
export declare function buildLocalFullArgs(ports: {
    registryPort: number;
    gitPort: number;
}, embeddedAgent: boolean, allowLocalhostWebhooks: boolean): string[];
export declare function generateSecureLocalMaterials(): {
    apiKey: string;
    certPath: string;
    keyPath: string;
};
export declare function mergeDockerDaemonJson(raw: string, registryAddr: string): string;
export declare function mergeNpmrc(raw: string, registryHost: string, registryPort: number, token: string): string;
export declare function mergeCargoConfig(raw: string, registryHost: string, registryPort: number, token: string): string;
export declare function mergeMavenSettings(raw: string, token: string): string;
export declare function runDoctorChecks(): Promise<PreflightCheckResult[]>;
export declare function runPostStartOnboarding(state: BootstrapState): Promise<void>;
export declare function prepareServerStart(options: StartSetupOptions): Promise<PreparedStart>;
//# sourceMappingURL=bootstrap.d.ts.map