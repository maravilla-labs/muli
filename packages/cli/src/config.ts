import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

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

export const DEFAULT_CONFIG: MuliConfig = {
  serverUrl: 'http://localhost:50051',
  tenantId: 'local',
  apiKey: null,
  tlsCaCertPath: null,
  registryHost: 'localhost',
  registryPort: 5000,
  gitHost: 'localhost',
  gitPort: 7000,
};

export function getConfigPath(): string {
  return path.join(os.homedir(), '.config', 'muli', 'config.json');
}

export function loadConfig(): MuliConfig {
  const configPath = getConfigPath();
  if (!fs.existsSync(configPath)) {
    return { ...DEFAULT_CONFIG };
  }
  try {
    const raw = fs.readFileSync(configPath, 'utf8');
    const parsed = JSON.parse(raw) as Partial<MuliConfig>;
    return { ...DEFAULT_CONFIG, ...parsed };
  } catch {
    return { ...DEFAULT_CONFIG };
  }
}

export function saveConfig(config: MuliConfig): void {
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
