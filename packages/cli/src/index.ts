#!/usr/bin/env node
import { Command } from 'commander';
import { registerAuthCommands } from './commands/auth.js';
import { registerRepoCommands } from './commands/repo.js';
import { registerJobCommands } from './commands/job.js';
import { registerRegistryCommands } from './commands/registry.js';
import { registerGitTokenCommands } from './commands/git-token.js';
import { registerConfigCommands } from './commands/config.js';
import { registerServerCommands } from './commands/server.js';
import { registerSetupCommands } from './commands/setup.js';

const program = new Command();

program
  .name('muli')
  .description('muli CLI — manage git repos, jobs, and registries')
  .version('0.1.0')
  .enablePositionalOptions()
  .passThroughOptions();

registerAuthCommands(program);
registerRepoCommands(program);
registerJobCommands(program);
registerRegistryCommands(program);
registerGitTokenCommands(program);
registerConfigCommands(program);
registerServerCommands(program);
registerSetupCommands(program);

program.parse(process.argv);
