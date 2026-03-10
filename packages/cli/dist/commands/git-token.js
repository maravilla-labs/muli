import chalk from 'chalk';
import { loadConfig } from '../config.js';
import { buildClients, callRpc } from '../grpc.js';
import { render } from 'ink';
import React from 'react';
import { Table } from '../ui/table.js';
export function registerGitTokenCommands(program) {
    const git = program
        .command('git')
        .description('Manage git access tokens');
    const token = git
        .command('token')
        .description('Manage git access tokens');
    token
        .command('create')
        .description('Create a new git access token')
        .option('-d, --description <desc>', 'Token description', '')
        .option('--permission <perm>', 'Permission level: PULL, PUSH, or ADMIN', 'PUSH')
        .action(async (opts) => {
        const config = loadConfig();
        const clients = buildClients(config);
        try {
            const res = await callRpc(clients.git, 'CreateAccessToken', {
                tenant_id: config.tenantId,
                description: opts.description,
                permissions: [`GIT_PERMISSION_${opts.permission.toUpperCase()}`],
            }, clients.meta);
            const tokenId = res.token_id ?? '?';
            const plaintext = res.plaintext_token ?? '(see server)';
            console.log(chalk.green('✓'), 'Git access token created:');
            console.log(`  ID:          ${chalk.bold(tokenId)}`);
            console.log(`  Token:       ${chalk.yellow(plaintext)}`);
            console.log(`  Description: ${opts.description || '(none)'}`);
            console.log(`  Permission:  ${opts.permission.toUpperCase()}`);
            console.log();
            console.log(chalk.dim('Use this token as your git password:'));
            console.log(chalk.dim(`  git clone http://<user>:${plaintext}@${config.gitHost}:${config.gitPort}/<namespace>/<repo>`));
            console.log(chalk.dim('\nStore this token securely — it will not be shown again.'));
        }
        catch (err) {
            console.error(chalk.red('Error creating git token:'), err.message);
            process.exit(1);
        }
    });
    token
        .command('list')
        .description('List git access tokens')
        .action(async () => {
        const config = loadConfig();
        const clients = buildClients(config);
        try {
            const res = await callRpc(clients.git, 'ListAccessTokens', { tenant_id: config.tenantId }, clients.meta);
            const tokens = res.tokens ?? [];
            if (tokens.length === 0) {
                console.log(chalk.dim('No git tokens found.'));
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
            console.error(chalk.red('Error listing git tokens:'), err.message);
            process.exit(1);
        }
    });
    token
        .command('revoke <token-id>')
        .description('Revoke a git access token')
        .action(async (tokenId) => {
        const config = loadConfig();
        const clients = buildClients(config);
        try {
            await callRpc(clients.git, 'RevokeAccessToken', { token_id: tokenId }, clients.meta);
            console.log(chalk.green('✓'), `Git token ${tokenId} revoked`);
        }
        catch (err) {
            console.error(chalk.red('Error revoking git token:'), err.message);
            process.exit(1);
        }
    });
}
//# sourceMappingURL=git-token.js.map