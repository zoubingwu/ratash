# Hopash RS

Hopash RS is a macOS command-line application for running and managing Mihomo. It provides scriptable commands and a Ratatui Status Interface for subscription Profiles, proxy selection, latency, traffic, Core Logs, and local routing rules.

The executable is named `hopash`. The first release supports Apple Silicon and Intel Macs.

## Project Status

Hopash RS is in pre-release development for macOS. Tagged releases pass the full contract, resource, packaging, signing, and notarization gates before publication.

## Installation

Download the `.pkg` and matching `.pkg.sha256` file for your Mac from [GitHub Releases](../../releases/latest):

- Apple Silicon: `hopash-0.1.0-aarch64-apple-darwin.pkg`
- Intel: `hopash-0.1.0-x86_64-apple-darwin.pkg`

Verify and install the package from its download directory:

```sh
PACKAGE='hopash-0.1.0-aarch64-apple-darwin.pkg'
shasum -a 256 -c "$PACKAGE.sha256"
sudo env HOPASH_OWNER_UID="$(id -u)" /usr/sbin/installer -pkg "$PACKAGE" -target /
```

The package installs the `hopash` command, the compatible Mihomo Core, the macOS service required for TUN operation, shell completions, the `hopash(1)` manual, and the Hopash AI Skill.

Verify the installation:

```sh
hopash --version
man hopash
```

## Usage

Start the background application, then add a remote HTTP(S) subscription:

```sh
hopash start
hopash profile add 'https://example.com/subscription.yaml'
hopash profile list
hopash profile use '<profile>'
```

Open the interactive Status Interface:

```sh
hopash status
```

Inspect status and select a proxy Node from scripts:

```sh
hopash status --json
hopash proxy list '<group>' --json
hopash proxy select '<group>' '<node>'
hopash latency list --json
```

Follow Core Logs and manage local routing rules:

```sh
hopash logs --follow
hopash rule list --json
hopash rule add 'DOMAIN,api.example.com,DIRECT' --before 'MATCH,PROXY'
```

Show the complete automation contract:

```sh
hopash help agent
```

Stop the background application:

```sh
hopash stop
```

## Shell Completion

Fish discovers the installed completion automatically. For Zsh, add the installed function directory to `fpath` before running `compinit`:

```zsh
fpath=(/usr/local/share/zsh/site-functions $fpath)
autoload -Uz compinit && compinit
```

For Bash, source the installed completion from the shell startup file:

```bash
source /usr/local/share/bash-completion/completions/hopash
```

## Uninstall

Stop Hopash RS and run the packaged uninstaller:

```sh
hopash stop
sudo /usr/local/share/hopash/uninstall.sh
```

The uninstaller preserves each user's saved Profiles and Local Rule Set under `~/Library/Application Support/Hopash RS`.
