export interface MuliConfig {
    serverUrl: string;
    tenantId: string;
    apiKey: string | null;
    tlsCaCertPath: string | null;
    registryHost: string;
    registryPort: number;
    gitHost: string;
    gitPort: number;
}
export declare const DEFAULT_CONFIG: MuliConfig;
export declare function getConfigPath(): string;
export declare function loadConfig(): MuliConfig;
export declare function saveConfig(config: MuliConfig): void;
//# sourceMappingURL=config.d.ts.map