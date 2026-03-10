import chalk from 'chalk';
import { execSync } from 'child_process';
import { loadConfig } from '../config.js';
import { buildClients, callRpc } from '../grpc.js';
import { render } from 'ink';
import React from 'react';
import { Table } from '../ui/table.js';
export function registerRepoCommands(program) {
    const repo = program
        .command('repo')
        .description('Manage git repositories');
    repo
        .command('create <name>')
        .description('Create a new repository')
        .option('-n, --namespace <ns>', 'Namespace (defaults to tenant ID)', '')
        .option('--private', 'Make repository private', false)
        .option('-d, --description <desc>', 'Repository description', '')
        .option('--init', 'Clone the empty repo locally after creation', false)
        .option('--default-branch <branch>', 'Default branch name', 'main')
        .action(async (name, opts) => {
        const config = loadConfig();
        const clients = buildClients(config);
        const namespace = opts.namespace || config.tenantId;
        try {
            const res = await callRpc(clients.git, 'CreateRepository', {
                tenant_id: config.tenantId,
                namespace,
                name,
                description: opts.description,
                is_private: opts.private,
            }, clients.meta);
            const repoUrl = `http://${config.gitHost}:${config.gitPort}/${namespace}/${name}`;
            console.log(chalk.green('✓'), `Repository created: ${chalk.bold(`${namespace}/${name}`)}`);
            console.log(`  ID:  ${res.id}`);
            console.log(`  URL: ${chalk.cyan(repoUrl)}`);
            if (opts.init) {
                console.log(chalk.dim(`\nCloning ${repoUrl}…`));
                try {
                    execSync(`git clone "${repoUrl}"`, { stdio: 'inherit' });
                    console.log(chalk.green('✓'), `Cloned into ./${name} — cd ${name} to get started`);
                }
                catch (cloneErr) {
                    console.warn(chalk.yellow('Warning:'), 'Clone failed (repo is empty). Use:');
                    console.warn(`  git clone ${repoUrl}`);
                }
            }
        }
        catch (err) {
            console.error(chalk.red('Error creating repository:'), err.message);
            process.exit(1);
        }
    });
    repo
        .command('list')
        .description('List repositories')
        .option('-n, --namespace <ns>', 'Filter by namespace')
        .option('--limit <n>', 'Maximum results', '50')
        .action(async (opts) => {
        const config = loadConfig();
        const clients = buildClients(config);
        try {
            const res = await callRpc(clients.git, 'ListRepositories', { tenant_id: config.tenantId }, clients.meta);
            const repos = res.repositories ?? [];
            if (repos.length === 0) {
                console.log(chalk.dim('No repositories found.'));
                return;
            }
            render(React.createElement((Table), {
                columns: [
                    { header: 'NAMESPACE', key: 'namespace', width: 20 },
                    { header: 'NAME', key: 'name', width: 24 },
                    { header: 'PRIVATE', key: 'is_private', width: 7, render: v => (v ? 'yes' : 'no') },
                    { header: 'BRANCH', key: 'default_branch', width: 12 },
                    { header: 'ID', key: 'id', width: 36 },
                ],
                data: repos,
            }));
        }
        catch (err) {
            console.error(chalk.red('Error listing repositories:'), err.message);
            process.exit(1);
        }
    });
    repo
        .command('clone <repo>')
        .description('Clone a repository (format: namespace/name)')
        .action(async (repoArg) => {
        const config = loadConfig();
        const parts = repoArg.split('/');
        if (parts.length !== 2) {
            console.error(chalk.red('Error: repo must be in the format namespace/name'));
            process.exit(1);
        }
        const [namespace, name] = parts;
        const repoUrl = `http://${config.gitHost}:${config.gitPort}/${namespace}/${name}`;
        console.log(chalk.dim(`Cloning ${repoUrl}…`));
        try {
            execSync(`git clone "${repoUrl}"`, { stdio: 'inherit' });
            console.log(chalk.green('✓'), `Cloned into ./${name}`);
        }
        catch (err) {
            console.error(chalk.red('Clone failed:'), err.message);
            process.exit(1);
        }
    });
}
//# sourceMappingURL=repo.js.map