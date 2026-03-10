import chalk from 'chalk';
import promptPassword from '@inquirer/password';
import { loadConfig, saveConfig } from '../config.js';
import { buildClients, callRpc } from '../grpc.js';
export function registerAuthCommands(program) {
    const auth = program
        .command('auth')
        .description('Manage authentication and server connection');
    auth
        .command('login [server-url]')
        .description('Connect to a muli server and save configuration')
        .action(async (serverUrl) => {
        const config = loadConfig();
        const url = serverUrl ?? config.serverUrl;
        console.log(chalk.dim(`Connecting to ${url}…`));
        // Build a temporary client to health-check
        const tmpConfig = { ...config, serverUrl: url, apiKey: null };
        let clients;
        try {
            clients = buildClients(tmpConfig);
        }
        catch (err) {
            console.error(chalk.red('Failed to load proto files:'), err.message);
            process.exit(1);
        }
        // Health check
        try {
            await callRpc(clients.health, 'Check', {}, clients.meta);
        }
        catch (err) {
            console.error(chalk.red(`Cannot reach server at ${url}:`), err.message);
            process.exit(1);
        }
        // Prompt for API key (optional)
        let apiKey = null;
        const keyInput = await promptPassword({
            message: 'API key (press Enter to skip for dev servers without auth):',
            mask: '*',
        }).catch(() => '');
        if (keyInput && keyInput.trim() !== '') {
            apiKey = keyInput.trim();
        }
        const newConfig = {
            ...config,
            serverUrl: url,
            apiKey,
        };
        saveConfig(newConfig);
        console.log(chalk.green('✓'), `Connected to ${chalk.bold(url)}, tenant: ${chalk.bold(newConfig.tenantId)}`);
    });
    auth
        .command('logout')
        .description('Clear the stored API key')
        .action(() => {
        const config = loadConfig();
        saveConfig({ ...config, apiKey: null });
        console.log(chalk.green('✓'), 'Logged out (API key cleared)');
    });
    auth
        .command('whoami')
        .description('Show current server and tenant configuration')
        .action(() => {
        const config = loadConfig();
        console.log(`Server:   ${chalk.bold(config.serverUrl)}`);
        console.log(`Tenant:   ${chalk.bold(config.tenantId)}`);
        console.log(`API key:  ${config.apiKey ? chalk.green('set') : chalk.dim('not set (dev mode)')}`);
        console.log(`Registry: ${chalk.bold(`${config.registryHost}:${config.registryPort}`)}`);
        console.log(`Git host: ${chalk.bold(`${config.gitHost}:${config.gitPort}`)}`);
    });
}
//# sourceMappingURL=auth.js.map