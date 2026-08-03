# Ratash

Ratash is a macOS command-line application for running and managing Mihomo. It provides scriptable commands and a Ratatui Status Interface for subscription Profiles, proxy selection, latency, traffic, Core Logs, and local routing rules.

The executable is named `ratash`. The first release supports Apple Silicon and Intel Macs.

## Project Status

Ratash is in pre-release development for macOS. Tagged releases pass the full contract, resource, packaging, signing, and notarization gates before publication.

## Installation

### Personal package

Build a complete package from a source checkout without an Apple Developer account:

```sh
./scripts/package-local-macos.sh --output dist
```

The script builds Ratash with the `local-unsigned` trust policy, downloads and verifies the pinned Mihomo Core, adds an ad-hoc code identity, and creates one installer plus its SHA-256 file. The resulting package requires no signing credentials. macOS may show a developer warning; approve the package in **System Settings > Privacy & Security** when prompted.

The personal trust policy requires `/usr/local` and `/usr/local/bin` to be root-owned and protected from group and other writes.

Copy the matching `*-local-unsigned.pkg` and `.pkg.sha256` files to the target Mac. Verify and install them from the download directory:

```sh
PACKAGE='ratash-0.1.1-aarch64-apple-darwin-local-unsigned.pkg'
shasum -a 256 -c "$PACKAGE.sha256"
sudo env RATASH_OWNER_UID="$(id -u)" /usr/sbin/installer -allowUntrusted -pkg "$PACKAGE" -target /
```

### Signed release package

Download the `.pkg` and matching `.pkg.sha256` file for your Mac from [GitHub Releases](../../releases/latest):

- Apple Silicon: `ratash-0.1.1-aarch64-apple-darwin.pkg`
- Intel: `ratash-0.1.1-x86_64-apple-darwin.pkg`

Verify and install the package from its download directory:

```sh
PACKAGE='ratash-0.1.1-aarch64-apple-darwin.pkg'
shasum -a 256 -c "$PACKAGE.sha256"
sudo env RATASH_OWNER_UID="$(id -u)" /usr/sbin/installer -pkg "$PACKAGE" -target /
```

The package installs the `ratash` command, the compatible Mihomo Core, the macOS service required for TUN operation, shell completions, the `ratash(1)` manual, and the Ratash AI Skill.

Verify the installation:

```sh
ratash --version
man ratash
```

## Usage

Start the background application, then add a remote HTTP(S) subscription:

```sh
ratash start
ratash profile add 'https://example.com/subscription.yaml'
ratash profile list
ratash profile use '<profile>'
```

Open the interactive Status Interface:

```sh
ratash status
```

Inspect status and select a proxy Node from scripts:

```sh
ratash status --json
ratash proxy list '<group>' --json
ratash proxy select '<group>' '<node>'
ratash latency list --json
```

Follow Core Logs and manage local routing rules:

```sh
ratash logs --follow
ratash rule list --json
ratash rule add 'DOMAIN,api.example.com,DIRECT' --before 'MATCH,PROXY'
```

Show the complete automation contract:

```sh
ratash help agent
```

Stop the background application:

```sh
ratash stop
```

## Shell Completion

Fish discovers the installed completion automatically. For Zsh, add the installed function directory to `fpath` before running `compinit`:

```zsh
fpath=(/usr/local/share/zsh/site-functions $fpath)
autoload -Uz compinit && compinit
```

For Bash, source the installed completion from the shell startup file:

```bash
source /usr/local/share/bash-completion/completions/ratash
```

## Uninstall

Stop Ratash and run the packaged uninstaller:

```sh
ratash stop
sudo /usr/local/share/ratash/uninstall.sh
```

The uninstaller preserves each user's saved Profiles and Local Rule Set under `~/Library/Application Support/ratash`.
