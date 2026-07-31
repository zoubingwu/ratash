# Hopash RS

Hopash RS is a lightweight Rust command-line client for running and managing Mihomo. It combines scriptable commands with a Ratatui terminal interface for profile management, proxy selection, latency checks, traffic monitoring, logs, and local routing rules.

The executable is named `hopash`.

## Project Status

Hopash RS is in the design phase. The first implementation release will target macOS.

## Installation

The first supported release will provide a macOS installer containing the `hopash` executable, the compatible Mihomo Core, and the service required for TUN operation. Verified download and installation commands will be published with that release.

After installation, verify the command:

```sh
hopash --version
```

## Usage

The following examples describe the planned MVP command surface.

Start Hopash RS and add a remote subscription:

```sh
hopash start
hopash profile add 'https://example.com/subscription.yaml'
hopash profile list
hopash profile use '<profile>'
```

Open the interactive terminal interface:

```sh
hopash status
```

Inspect status and switch a proxy node from scripts:

```sh
hopash status --json
hopash proxy list '<group>' --json
hopash proxy select '<group>' '<node>'
hopash latency list --json
```

Follow Core logs and manage local routing rules:

```sh
hopash logs --follow
hopash rule list --json
hopash rule add 'DOMAIN,api.example.com,DIRECT' --before 'MATCH,PROXY'
```

Stop Hopash RS:

```sh
hopash stop
```
