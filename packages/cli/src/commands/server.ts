import { Command } from 'commander';
import chalk from 'chalk';
import {
  getServerStatus,
  installServer,
  startServer,
  stopManagedServer,
  updateServer,
} from '../server/manager.js';
import { loadBootstrapState, prepareServerStart, runPostStartOnboarding } from '../setup/bootstrap.js';

export function registerServerCommands(program: Command): void {
  const server = program
    .command('server')
    .description('Install, run, and update the local muli-server binary');

  server
    .command('install')
    .description('Download and install muli-server binary for this platform')
    .option('--version <version>', 'install a specific version (e.g. 0.1.0)')
    .option('--force', 'reinstall even if version is already installed', false)
    .action(async (opts: { version?: string; force?: boolean }) => {
      try {
        const result = await installServer(opts.version, !!opts.force);
        if (result.changed) {
          console.log(chalk.green('✓'), `Installed muli-server ${result.version}`);
        } else {
          console.log(chalk.yellow('•'), `muli-server ${result.version} is already installed`);
        }
        console.log(chalk.dim(`Binary: ${result.binaryPath}`));
      } catch (err: any) {
        console.error(chalk.red('Install failed:'), err.message);
        process.exit(1);
      }
    });

  server
    .command('start')
    .description('Start local muli-server (defaults to local full stack profile)')
    .option('--version <version>', 'install/use specific server version')
    .option('--embedded-agent', 'start server with embedded agent')
    .option('--detach', 'run in background (default)', true)
    .option('--force', 'stop managed running server and replace it', false)
    .option('--no-setup', 'skip first-run setup wizard')
    .option('--setup', 'force rerun first-run setup wizard', false)
    .argument('[serverArgs...]', 'additional muli-server args after --')
    .action(async (serverArgs: string[], opts: {
      version?: string;
      embeddedAgent?: boolean;
      detach?: boolean;
      force?: boolean;
      setup?: boolean;
      noSetup?: boolean;
    }) => {
      try {
        const forceSetupFlag = process.argv.includes('--setup');
        const noSetupFlag = process.argv.includes('--no-setup');
        const prepared = await prepareServerStart({
          forceSetup: forceSetupFlag,
          noSetup: noSetupFlag,
          localFullByDefault: true,
          requestedEmbeddedAgent: opts.embeddedAgent,
          userArgs: serverArgs ?? [],
        });

        const result = await startServer({
          version: opts.version,
          embeddedAgent: opts.embeddedAgent,
          detach: opts.detach !== false,
          extraArgs: prepared.extraArgs,
          force: !!opts.force,
          env: prepared.env,
        });

        if (opts.detach !== false) {
          console.log(chalk.green('✓'), `Started muli-server ${result.version}`);
          if (result.pid) {
            console.log(chalk.dim(`PID: ${result.pid}`));
          }
          if (prepared.didSetupRun) {
            const state = loadBootstrapState();
            try {
              await runPostStartOnboarding(state);
            } catch (err: any) {
              console.error(chalk.yellow('Onboarding warning:'), err.message);
              console.log(chalk.dim('You can rerun onboarding with: muli setup rerun'));
            }
          }
        }
      } catch (err: any) {
        console.error(chalk.red('Start failed:'), err.message);
        process.exit(1);
      }
    });

  server
    .command('stop')
    .description('Stop managed local muli-server process')
    .option('--force', 'force kill if graceful stop times out', false)
    .action(async (opts: { force?: boolean }) => {
      try {
        const result = await stopManagedServer(!!opts.force);
        if (result.stopped) {
          console.log(chalk.green('✓'), `Stopped muli-server (PID ${result.pid})`);
        } else {
          console.log(chalk.yellow('•'), 'No managed running server found');
        }
      } catch (err: any) {
        console.error(chalk.red('Stop failed:'), err.message);
        process.exit(1);
      }
    });

  server
    .command('status')
    .description('Show installed/running version and update state')
    .action(async () => {
      try {
        const status = await getServerStatus();
        console.log(`Installed version: ${status.installedVersion ?? chalk.dim('not installed')}`);
        console.log(`Binary path:       ${status.binaryPath ?? chalk.dim('n/a')}`);
        console.log(`Running:           ${status.running ? chalk.green('yes') : chalk.dim('no')}`);
        if (status.running) {
          console.log(`PID:               ${status.pid}`);
          console.log(`Started at:        ${status.startedAt}`);
        }
        console.log(`Latest version:    ${status.latestVersion ?? chalk.dim('unknown')}`);
        console.log(`Update state:      ${status.updateState}`);
        console.log(`Setup state:       ${status.setupStatus}`);
      } catch (err: any) {
        console.error(chalk.red('Status failed:'), err.message);
        process.exit(1);
      }
    });

  server
    .command('update')
    .description('Update installed muli-server binary')
    .option('--to <version>', 'update to a specific version')
    .option('--check', 'only check whether an update is available', false)
    .option('--force', 'allow update while managed server is running (stop/update/restart)', false)
    .action(async (opts: { to?: string; check?: boolean; force?: boolean }) => {
      try {
        const result = await updateServer({
          toVersion: opts.to,
          check: !!opts.check,
          force: !!opts.force,
        });

        if (result.action === 'checked') {
          console.log(`Installed: ${result.installedVersion ?? 'not installed'}`);
          console.log(`Latest:    ${result.targetVersion ?? 'unknown'}`);
          console.log(`State:     ${result.updateState}`);
          return;
        }

        if (result.action === 'noop') {
          console.log(chalk.green('✓'), `Already up to date (${result.installedVersion})`);
          return;
        }

        console.log(chalk.green('✓'), `Updated muli-server to ${result.installedVersion}`);
        if (result.restarted) {
          console.log(chalk.dim('Managed server was restarted after update (--force).'));
        }
      } catch (err: any) {
        console.error(chalk.red('Update failed:'), err.message);
        process.exit(1);
      }
    });
}
