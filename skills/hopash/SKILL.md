---
name: hopash
description: Operate a local Hopash RS installation through its stable command-line contract. Use when an agent needs to inspect Hopash, manage subscription Profiles, select Mihomo proxy Nodes, query latency or logs, or apply one precise Local Rule Set mutation.
---

# Hopash

Use the installed `hopash` executable as the only product interface. Keep state files, runtime bundles, sockets, and the Managed Core process under Supervisor ownership.

## Start with the live contract

1. Run `hopash help agent` before the first operation in a session.
2. Use `--json` for queries, lifecycle commands, and mutations.
3. Read stdout as one versioned success envelope. Read stderr as one versioned error envelope.
4. Treat lifecycle, Profile, Proxy, and rule mutations as state-changing operations. Execute the exact mutation requested by the user.

## Inspect before changing state

- Use `hopash status --json` for Supervisor, Core, TUN, Active Profile, selected Node, traffic, and Runtime Generation.
- Use `hopash profile list --json` before choosing or removing a Profile.
- Use `hopash proxy list '<group>' --json` before selecting a Node.
- Use `hopash latency list --json` or `hopash latency show '<node>' --json` for probe state.
- Use `hopash rule list --json` immediately before every rule mutation.

Prefer opaque IDs from JSON responses. A case-sensitive unique display name is suitable for an interactive request. When an error returns candidates, select only the candidate identified by the user.

## Change one rule

1. Copy the complete, case-sensitive Rule String for the target or anchor from `hopash rule list --json`.
2. Perform one operation with `hopash rule add`, `hopash rule replace`, or `hopash rule remove`.
3. For add, provide exactly one placement: `--prepend`, `--append`, `--before '<anchor>'`, or `--after '<anchor>'`.
4. Inspect the Runtime Apply and recovery fields in the response.
5. After `rule_busy`, `rule_not_found`, `rule_ambiguous`, or `rule_already_exists`, read the current rule list again before retrying the complete operation.

## Recover from failures

- For `supervisor_unavailable`, run `hopash start --json` when starting Hopash is within the user's request, then verify with `hopash status --json`.
- After a Runtime Apply failure, inspect `hopash status --json` and reread the affected resource. The response identifies the candidate, committed generation, and recovery outcome.
- Treat `retryable: true` as permission to refresh state and repeat the complete requested operation within a bounded retry policy.
- Preserve Subscription URL credentials. Do not print, log, or copy a raw URL from local state or diagnostics.

For live logs, run `hopash logs --follow --json` and consume one versioned NDJSON event per line until the requested observation is complete or the user interrupts the command.
