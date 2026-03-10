# @maravilla-labs/muli

CLI for running and managing a local [Muli](https://github.com/maravilla-labs/muli) server.

## Install

```bash
npm install -g @maravilla-labs/muli
```

## Quickstart

```bash
# Install server binary for your platform
muli server install

# Start local stack (first run includes setup wizard)
muli server start

# Optional: run in background
muli server start --detach

# Connect CLI to local server
muli auth login http://localhost:50051

# Run a test job
muli job run --image alpine -- sh -c "echo hello from muli"
```

## Core Commands

```bash
muli server status
muli server update --check
muli setup doctor
muli setup rerun
```

## Package Notes

- Package name: `@maravilla-labs/muli`
- Binary command: `muli`
- Node.js: `>=18`

## License

MIT OR Apache-2.0
