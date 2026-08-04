# Ratash

Ratash is an AI-agent-friendly TUI Clash client for macOS, powered by Mihomo and Ratatui. Use it interactively in the terminal or through scriptable JSON commands.

Apple Silicon and Intel Macs are supported.

## Install

```sh
curl -fsSL https://ratash.zoubingwu.com/install.sh | sh
```

The installer selects the package for your Mac, requests `sudo`, installs Ratash, and starts it.

## Use

Add and activate a subscription:

```sh
ratash profile add 'https://example.com/subscription.yaml'
ratash profile list
ratash profile use '<profile>'
```

Open the TUI:

```sh
ratash status
```

Use `↑`/`↓` to move, `Enter` to select, `:` to open commands, and `q` to quit.

Use Ratash from scripts or AI agents:

```sh
ratash status --json
ratash proxy list '<group>' --json
ratash proxy select '<group>' '<node>'
ratash logs --follow
ratash help agent
```

Stop Ratash:

```sh
ratash stop
```

## Update

```sh
curl -fsSL https://ratash.zoubingwu.com/install.sh | sh -s -- update
```

## Uninstall

```sh
curl -fsSL https://ratash.zoubingwu.com/install.sh | sh -s -- uninstall
```

Saved Profiles and Local Rules are preserved.
