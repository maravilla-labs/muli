import chalk from 'chalk';
import { getServerStatus } from '../server/manager.js';
import { loadBootstrapState, prepareServerStart, runDoctorChecks, runPostStartOnboarding } from '../setup/bootstrap.js';
export function registerSetupCommands(program) {
    const setup = program
        .command('setup')
        .description('Setup diagnostics and first-run wizard controls');
    setup
        .command('doctor')
        .description('Run prerequisite checks for local full-stack profile')
        .action(async () => {
        try {
            const checks = await runDoctorChecks();
            let failed = 0;
            for (const check of checks) {
                if (check.ok) {
                    console.log(chalk.green('✓'), `${check.name}: ${check.message}`);
                }
                else {
                    failed += 1;
                    console.log(chalk.red('✗'), `${check.name}: ${check.message}`);
                    if (check.remediation) {
                        console.log(chalk.yellow('  ->'), check.remediation);
                    }
                }
            }
            if (failed > 0) {
                process.exit(1);
            }
        }
        catch (err) {
            console.error(chalk.red('Doctor failed:'), err.message);
            process.exit(1);
        }
    });
    setup
        .command('rerun')
        .description('Rerun first-run wizard and optional client onboarding')
        .action(async () => {
        try {
            const status = await getServerStatus();
            await prepareServerStart({
                forceSetup: true,
                noSetup: false,
                localFullByDefault: true,
                requestedEmbeddedAgent: true,
                userArgs: [],
                preserveExistingPorts: status.running,
            });
            console.log(chalk.green('✓'), 'Setup profile refreshed.');
            if (status.running) {
                await runPostStartOnboarding(loadBootstrapState());
            }
            else {
                console.log(chalk.dim('Server is not running. Start it with `muli server start` to complete onboarding.'));
            }
        }
        catch (err) {
            console.error(chalk.red('Setup rerun failed:'), err.message);
            process.exit(1);
        }
    });
}
//# sourceMappingURL=setup.js.map