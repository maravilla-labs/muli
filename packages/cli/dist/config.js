import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
export const DEFAULT_CONFIG = {
    serverUrl: 'http://localhost:50051',
    tenantId: 'local',
    apiKey: null,
    tlsCaCertPath: null,
    registryHost: 'local.localhost',
    registryPort: 5000,
    gitHost: 'local.localhost',
    gitPort: 7000,
};
export function getConfigPath() {
    return path.join(os.homedir(), '.config', 'muli', 'config.json');
}
export function loadConfig() {
    const configPath = getConfigPath();
    if (!fs.existsSync(configPath)) {
        return { ...DEFAULT_CONFIG };
    }
    try {
        const raw = fs.readFileSync(configPath, 'utf8');
        const parsed = JSON.parse(raw);
        return { ...DEFAULT_CONFIG, ...parsed };
    }
    catch {
        return { ...DEFAULT_CONFIG };
    }
}
export function saveConfig(config) {
    const configPath = getConfigPath();
    const dir = path.dirname(configPath);
    if (!fs.existsSync(dir)) {
        fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
    }
    fs.writeFileSync(configPath, JSON.stringify(config, null, 2) + '\n', {
        mode: 0o600,
        encoding: 'utf8',
    });
}
//# sourceMappingURL=config.js.map