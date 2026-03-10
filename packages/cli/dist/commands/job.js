import chalk from 'chalk';
import { render } from 'ink';
import React from 'react';
import { loadConfig } from '../config.js';
import { buildClients, callRpc } from '../grpc.js';
import { LogStream } from '../ui/log-stream.js';
import { Table } from '../ui/table.js';
export function registerJobCommands(program) {
    const job = program
        .command('job')
        .description('Manage and monitor jobs');
    job
        .command('run')
        .description('Submit and stream a job')
        .requiredOption('-i, --image <image>', 'Container image')
        .option('-e, --env <KV...>', 'Environment variables (KEY=VALUE)', [])
        .option('--cpu <millicores>', 'CPU request (e.g. 500m)', '')
        .option('--mem <bytes>', 'Memory request (e.g. 128Mi)', '')
        .option('--timeout <seconds>', 'Job timeout in seconds', '0')
        .allowExcessArguments(true)
        .argument('[cmd...]', 'Command to run inside the container')
        .action(async (cmdArgs, opts, cmd) => {
        const config = loadConfig();
        const clients = buildClients(config);
        // Parse env vars
        const envVars = opts.env.map((kv) => {
            const idx = kv.indexOf('=');
            if (idx === -1)
                return { name: kv, value: '' };
            return { name: kv.slice(0, idx), value: kv.slice(idx + 1) };
        });
        // Build resource spec
        const resources = {};
        if (opts.cpu)
            resources.cpu_millis = parseCpu(opts.cpu);
        if (opts.mem)
            resources.memory_bytes = parseMemory(opts.mem);
        // Combine: args after -- separator
        const rawArgs = cmd.args ?? [];
        const separatorIdx = rawArgs.indexOf('--');
        const command = separatorIdx !== -1
            ? rawArgs.slice(separatorIdx + 1)
            : (cmdArgs ?? []);
        try {
            const res = await callRpc(clients.job, 'SubmitJob', {
                tenant_id: config.tenantId,
                runner_image: opts.image,
                deployment_id: 'cli',
                project_id: 'cli',
                workspace_id: 'cli',
                env_vars: [
                    ...envVars,
                    ...(command.length > 0 ? [{ name: 'MULI_CMD', value: command.join(' ') }] : []),
                ],
                resources: Object.keys(resources).length > 0 ? resources : undefined,
                timeout_seconds: parseInt(opts.timeout, 10),
                priority_tier: 'PRIORITY_TIER_STANDARD',
            }, clients.meta);
            const jobId = res.job_id ?? res.id ?? '';
            console.log(chalk.green('✓'), `Job submitted: ${chalk.bold(jobId)}`);
            console.log();
            // Stream logs with Ink TUI
            let exitCode = 0;
            const { waitUntilExit } = render(React.createElement(LogStream, {
                jobId,
                clients,
                follow: true,
                onDone: (code) => { exitCode = code; },
            }));
            await waitUntilExit();
            process.exit(exitCode);
        }
        catch (err) {
            console.error(chalk.red('Error submitting job:'), err.message);
            process.exit(1);
        }
    });
    job
        .command('list')
        .description('List jobs')
        .option('--state <state>', 'Filter by state (e.g. RUNNING, SUCCEEDED)')
        .option('--limit <n>', 'Maximum results', '20')
        .action(async (opts) => {
        const config = loadConfig();
        const clients = buildClients(config);
        try {
            const res = await callRpc(clients.job, 'ListJobs', {
                tenant_id: config.tenantId,
                state_filter: opts.state ? `JOB_STATE_${opts.state.toUpperCase()}` : '',
                limit: parseInt(opts.limit, 10),
            }, clients.meta);
            const jobs = res.jobs ?? [];
            if (jobs.length === 0) {
                console.log(chalk.dim('No jobs found.'));
                return;
            }
            render(React.createElement((Table), {
                columns: [
                    { header: 'JOB ID', key: 'job_id', width: 36 },
                    { header: 'IMAGE', key: 'image', width: 28 },
                    { header: 'STATE', key: 'state', width: 12 },
                    {
                        header: 'SUBMITTED',
                        key: 'created_at',
                        width: 20,
                        render: v => (v ? new Date(Number(v) * 1000).toLocaleString() : '?'),
                    },
                ],
                data: jobs,
            }));
        }
        catch (err) {
            console.error(chalk.red('Error listing jobs:'), err.message);
            process.exit(1);
        }
    });
    job
        .command('status <job-id>')
        .description('Get the status of a job')
        .action(async (jobId) => {
        const config = loadConfig();
        const clients = buildClients(config);
        try {
            const res = await callRpc(clients.job, 'GetDetailedJobStatus', { job_id: jobId }, clients.meta);
            console.log(`Job ID:    ${chalk.bold(jobId)}`);
            console.log(`State:     ${colorState(res.state ?? res.job?.state)}`);
            console.log(`Image:     ${res.image ?? res.job?.image ?? '?'}`);
            if (res.exit_code !== undefined && res.exit_code !== null) {
                console.log(`Exit code: ${res.exit_code}`);
            }
            if (res.message) {
                console.log(`Message:   ${res.message}`);
            }
        }
        catch (err) {
            console.error(chalk.red('Error getting job status:'), err.message);
            process.exit(1);
        }
    });
    job
        .command('logs <job-id>')
        .description('Stream or fetch logs for a job')
        .option('-f, --follow', 'Follow log stream in real-time', false)
        .action(async (jobId, opts) => {
        const config = loadConfig();
        const clients = buildClients(config);
        if (opts.follow) {
            let exitCode = 0;
            const { waitUntilExit } = render(React.createElement(LogStream, {
                jobId,
                clients,
                follow: true,
                onDone: (code) => { exitCode = code; },
            }));
            await waitUntilExit();
            process.exit(exitCode);
        }
        else {
            // Fetch historical logs
            try {
                const res = await callRpc(clients.log, 'GetLogs', { job_id: jobId }, clients.meta);
                const entries = res.entries ?? [];
                if (entries.length === 0) {
                    console.log(chalk.dim('No logs found.'));
                    return;
                }
                for (const entry of entries) {
                    const streamLabel = String(entry.stream ?? '').toUpperCase() === 'STDERR'
                        ? chalk.red('[stderr]')
                        : chalk.dim('[stdout]');
                    console.log(`${streamLabel} ${entry.line}`);
                }
            }
            catch (err) {
                console.error(chalk.red('Error fetching logs:'), err.message);
                process.exit(1);
            }
        }
    });
    job
        .command('cancel <job-id>')
        .description('Cancel a running job')
        .action(async (jobId) => {
        const config = loadConfig();
        const clients = buildClients(config);
        try {
            await callRpc(clients.job, 'CancelJob', { job_id: jobId }, clients.meta);
            console.log(chalk.green('✓'), `Job ${jobId} cancelled`);
        }
        catch (err) {
            console.error(chalk.red('Error cancelling job:'), err.message);
            process.exit(1);
        }
    });
}
// ── helpers ────────────────────────────────────────────────────────────────
function colorState(state) {
    if (!state)
        return chalk.dim('unknown');
    switch (state.toUpperCase()) {
        case 'SUCCEEDED': return chalk.green(state);
        case 'RUNNING': return chalk.cyan(state);
        case 'PULLING': return chalk.cyan(state);
        case 'PENDING': return chalk.yellow(state);
        case 'SCHEDULED': return chalk.yellow(state);
        case 'FAILED': return chalk.red(state);
        case 'CANCELLED': return chalk.gray(state);
        case 'TIMED_OUT': return chalk.red(state);
        default: return state;
    }
}
/** Parse CPU string like "500m" or "2" into millicores integer */
function parseCpu(cpu) {
    if (cpu.endsWith('m'))
        return parseInt(cpu.slice(0, -1), 10);
    return Math.round(parseFloat(cpu) * 1000);
}
/** Parse memory string like "128Mi", "1Gi", "512" into bytes */
function parseMemory(mem) {
    const units = {
        Ki: 1024,
        Mi: 1024 ** 2,
        Gi: 1024 ** 3,
        Ti: 1024 ** 4,
        K: 1000,
        M: 1000 ** 2,
        G: 1000 ** 3,
        T: 1000 ** 4,
    };
    for (const [suffix, factor] of Object.entries(units)) {
        if (mem.endsWith(suffix)) {
            return Math.round(parseFloat(mem.slice(0, -suffix.length)) * factor);
        }
    }
    return parseInt(mem, 10);
}
//# sourceMappingURL=job.js.map