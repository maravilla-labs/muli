import { Command } from 'commander';
import chalk from 'chalk';
import { loadConfig, saveConfig, getConfigPath } from '../config.js';

const NUMERIC_KEYS = new Set(['registryPort', 'gitPort']);
const VALID_KEYS = [
  'serverUrl',
  'tenantId',
  'apiKey',
  'tlsCaCertPath',
  'registryHost',
  'registryPort',
  'gitHost',
  'gitPort',
];

export function registerConfigCommands(program: Command): void {
  const cfg = program.command('config').description('View and set CLI configuration');

  cfg
    .command('get [key]')
    .description('Print one or all config values')
    .action((key?: string) => {
      const config = loadConfig();
      if (key) {
        if (!VALID_KEYS.includes(key)) {
          console.error(chalk.red(`Unknown key: ${key}`));
          console.error(`Valid keys: ${VALID_KEYS.join(', ')}`);
          process.exit(1);
        }
        console.log((config as any)[key] ?? '(not set)');
      } else {
        console.log(chalk.dim(`Config file: ${getConfigPath()}`));
        for (const k of VALID_KEYS) {
          console.log(`  ${chalk.bold(k)}: ${(config as any)[k] ?? chalk.dim('(default)')}`);
        }
      }
    });

  cfg
    .command('set <key> <value>')
    .description('Persistently set a config value')
    .action((key: string, value: string) => {
      if (!VALID_KEYS.includes(key)) {
        console.error(chalk.red(`Unknown key: ${key}`));
        console.error(`Valid keys: ${VALID_KEYS.join(', ')}`);
        process.exit(1);
      }
      const config = loadConfig();
      const coerced: any = NUMERIC_KEYS.has(key) ? parseInt(value, 10) : value;
      (config as any)[key] = coerced;
      saveConfig(config);
      console.log(chalk.green('✓'), `${key} = ${coerced}`);
      console.log(chalk.dim(`Saved to ${getConfigPath()}`));
    });
}
