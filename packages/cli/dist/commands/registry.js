import chalk from 'chalk';
import { execSync } from 'child_process';
import { loadConfig } from '../config.js';
import { buildClients, callRpc } from '../grpc.js';
import { render } from 'ink';
import React from 'react';
import { Table } from '../ui/table.js';
export function registerRegistryCommands(program) {
    const registry = program
        .command('registry')
        .description('Manage Docker/npm registry tokens');
    const token = registry
        .command('token')
        .description('Manage registry tokens');
    token
        .command('create')
        .description('Create a new registry token')
        .option('-d, --description <desc>', 'Token description', '')
        .option('--ttl <seconds>', 'Token TTL in seconds (0 = no expiry)', '0')
        .option('--permission <perm>', 'Permission level: PULL, PUSH, or ADMIN', 'PUSH')
        .action(async (opts) => {
        const config = loadConfig();
        const clients = buildClients(config);
        try {
            const res = await callRpc(clients.registry, 'CreateRegistryToken', {
                tenant_id: config.tenantId,
                description: opts.description,
                ttl_seconds: parseInt(opts.ttl, 10) || undefined,
                permissions: [`REGISTRY_PERMISSION_${opts.permission.toUpperCase()}`],
            }, clients.meta);
            console.log(chalk.green('✓'), 'Registry token created:');
            console.log(`  ID:          ${chalk.bold(res.token_id ?? '?')}`);
            console.log(`  Token:       ${chalk.yellow(res.plaintext_token ?? '(see server)')}`);
            console.log(`  Description: ${opts.description || '(none)'}`);
            console.log(chalk.dim('\nStore this token securely — it will not be shown again.'));
        }
        catch (err) {
            console.error(chalk.red('Error creating token:'), err.message);
            process.exit(1);
        }
    });
    token
        .command('list')
        .description('List registry tokens')
        .action(async () => {
        const config = loadConfig();
        const clients = buildClients(config);
        try {
            const res = await callRpc(clients.registry, 'ListRegistryTokens', { tenant_id: config.tenantId }, clients.meta);
            const tokens = res.tokens ?? [];
            if (tokens.length === 0) {
                console.log(chalk.dim('No registry tokens found.'));
                return;
            }
            render(React.createElement((Table), {
                columns: [
                    { header: 'ID', key: 'id', width: 36 },
                    { header: 'DESCRIPTION', key: 'description', width: 28 },
                    { header: 'PERMISSION', key: 'permission', width: 12 },
                    {
                        header: 'CREATED',
                        key: 'created_at',
                        width: 20,
                        render: v => (v ? new Date(Number(v) * 1000).toLocaleString() : '?'),
                    },
                ],
                data: tokens,
            }));
        }
        catch (err) {
            console.error(chalk.red('Error listing tokens:'), err.message);
            process.exit(1);
        }
    });
    token
        .command('revoke <token-id>')
        .description('Revoke a registry token')
        .action(async (tokenId) => {
        const config = loadConfig();
        const clients = buildClients(config);
        try {
            await callRpc(clients.registry, 'RevokeRegistryToken', { token_id: tokenId }, clients.meta);
            console.log(chalk.green('✓'), `Token ${tokenId} revoked`);
        }
        catch (err) {
            console.error(chalk.red('Error revoking token:'), err.message);
            process.exit(1);
        }
    });
    registry
        .command('docker-login')
        .description('Run docker login against the local registry using a fresh token')
        .option('-d, --description <desc>', 'Token description', 'docker-login')
        .action(async (opts) => {
        const config = loadConfig();
        const clients = buildClients(config);
        const registryAddr = `${config.registryHost}:${config.registryPort}`;
        console.log(chalk.dim('Creating a registry token for docker login…'));
        let plaintext;
        try {
            const res = await callRpc(clients.registry, 'CreateRegistryToken', {
                tenant_id: config.tenantId,
                description: opts.description,
                ttl_seconds: 0,
                permissions: ['REGISTRY_PERMISSION_PULL', 'REGISTRY_PERMISSION_PUSH'],
            }, clients.meta);
            plaintext = res.plaintext_token ?? '';
            if (!plaintext) {
                throw new Error('Server did not return a plaintext token');
            }
        }
        catch (err) {
            console.error(chalk.red('Error creating token:'), err.message);
            process.exit(1);
        }
        console.log(chalk.dim(`Running: docker login ${registryAddr}`));
        try {
            execSync(`echo "${plaintext}" | docker login "${registryAddr}" --username muli --password-stdin`, { stdio: 'inherit' });
            console.log(chalk.green('✓'), `Logged in to ${registryAddr}`);
        }
        catch (err) {
            console.error(chalk.red('docker login failed:'), err.message);
            process.exit(1);
        }
    });
}
//# sourceMappingURL=registry.js.map