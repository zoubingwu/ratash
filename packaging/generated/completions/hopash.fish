# Print an optspec for argparse to handle cmd's options that are independent of any subcommand.
function __fish_hopash_global_optspecs
    string join \n h/help V/version
end

function __fish_hopash_needs_command
    # Figure out if the current invocation already has a command.
    set -l cmd (commandline -opc)
    set -e cmd[1]
    argparse -s (__fish_hopash_global_optspecs) -- $cmd 2>/dev/null
    or return
    if set -q argv[1]
        # Also print the command, so this can be used to figure out what it is.
        echo $argv[1]
        return 1
    end
    return 0
end

function __fish_hopash_using_subcommand
    set -l cmd (__fish_hopash_needs_command)
    test -z "$cmd"
    and return 1
    contains -- $cmd[1] $argv
end

complete -c hopash -n "__fish_hopash_needs_command" -s h -l help -d 'Print help'
complete -c hopash -n "__fish_hopash_needs_command" -s V -l version -d 'Print version'
complete -c hopash -n "__fish_hopash_needs_command" -f -a "start" -d 'Start the background Supervisor and wait until it is ready'
complete -c hopash -n "__fish_hopash_needs_command" -f -a "stop" -d 'Stop the Supervisor and its Managed Core'
complete -c hopash -n "__fish_hopash_needs_command" -f -a "restart" -d 'Restart the Supervisor and restore its committed runtime'
complete -c hopash -n "__fish_hopash_needs_command" -f -a "status" -d 'Open the Status Interface or print one JSON status snapshot'
complete -c hopash -n "__fish_hopash_needs_command" -f -a "profile" -d 'Add, list, activate, and remove subscription Profiles'
complete -c hopash -n "__fish_hopash_needs_command" -f -a "proxy" -d 'List Proxy Groups and select Nodes'
complete -c hopash -n "__fish_hopash_needs_command" -f -a "latency" -d 'Inspect latency for Active Profile Nodes'
complete -c hopash -n "__fish_hopash_needs_command" -f -a "rule" -d 'List and atomically mutate the Local Rule Set'
complete -c hopash -n "__fish_hopash_needs_command" -f -a "logs" -d 'Follow the live Core Log stream'
complete -c hopash -n "__fish_hopash_needs_command" -f -a "help" -d 'Show command help or the AI Agent operation contract'
complete -c hopash -n "__fish_hopash_using_subcommand start" -l json -d 'Emit a versioned JSON document or NDJSON stream'
complete -c hopash -n "__fish_hopash_using_subcommand start" -s h -l help -d 'Print help'
complete -c hopash -n "__fish_hopash_using_subcommand stop" -l json -d 'Emit a versioned JSON document or NDJSON stream'
complete -c hopash -n "__fish_hopash_using_subcommand stop" -s h -l help -d 'Print help'
complete -c hopash -n "__fish_hopash_using_subcommand restart" -l json -d 'Emit a versioned JSON document or NDJSON stream'
complete -c hopash -n "__fish_hopash_using_subcommand restart" -s h -l help -d 'Print help'
complete -c hopash -n "__fish_hopash_using_subcommand status" -l json -d 'Emit a versioned JSON document or NDJSON stream'
complete -c hopash -n "__fish_hopash_using_subcommand status" -s h -l help -d 'Print help'
complete -c hopash -n "__fish_hopash_using_subcommand profile; and not __fish_seen_subcommand_from add list use remove" -s h -l help -d 'Print help'
complete -c hopash -n "__fish_hopash_using_subcommand profile; and not __fish_seen_subcommand_from add list use remove" -f -a "add" -d 'Download, validate, and save an HTTP(S) subscription'
complete -c hopash -n "__fish_hopash_using_subcommand profile; and not __fish_seen_subcommand_from add list use remove" -f -a "list" -d 'List saved Profiles and their refresh state'
complete -c hopash -n "__fish_hopash_using_subcommand profile; and not __fish_seen_subcommand_from add list use remove" -f -a "use" -d 'Activate a validated Profile Snapshot'
complete -c hopash -n "__fish_hopash_using_subcommand profile; and not __fish_seen_subcommand_from add list use remove" -f -a "remove" -d 'Remove an Inactive Profile'
complete -c hopash -n "__fish_hopash_using_subcommand profile; and __fish_seen_subcommand_from add" -l json -d 'Emit a versioned JSON document or NDJSON stream'
complete -c hopash -n "__fish_hopash_using_subcommand profile; and __fish_seen_subcommand_from add" -s h -l help -d 'Print help'
complete -c hopash -n "__fish_hopash_using_subcommand profile; and __fish_seen_subcommand_from list" -l json -d 'Emit a versioned JSON document or NDJSON stream'
complete -c hopash -n "__fish_hopash_using_subcommand profile; and __fish_seen_subcommand_from list" -s h -l help -d 'Print help'
complete -c hopash -n "__fish_hopash_using_subcommand profile; and __fish_seen_subcommand_from use" -l json -d 'Emit a versioned JSON document or NDJSON stream'
complete -c hopash -n "__fish_hopash_using_subcommand profile; and __fish_seen_subcommand_from use" -s h -l help -d 'Print help'
complete -c hopash -n "__fish_hopash_using_subcommand profile; and __fish_seen_subcommand_from remove" -l json -d 'Emit a versioned JSON document or NDJSON stream'
complete -c hopash -n "__fish_hopash_using_subcommand profile; and __fish_seen_subcommand_from remove" -s h -l help -d 'Print help'
complete -c hopash -n "__fish_hopash_using_subcommand proxy; and not __fish_seen_subcommand_from list select" -s h -l help -d 'Print help'
complete -c hopash -n "__fish_hopash_using_subcommand proxy; and not __fish_seen_subcommand_from list select" -f -a "list" -d 'List the Nodes exposed by one Proxy Group'
complete -c hopash -n "__fish_hopash_using_subcommand proxy; and not __fish_seen_subcommand_from list select" -f -a "select" -d 'Select one Node in a Proxy Group'
complete -c hopash -n "__fish_hopash_using_subcommand proxy; and __fish_seen_subcommand_from list" -l json -d 'Emit a versioned JSON document or NDJSON stream'
complete -c hopash -n "__fish_hopash_using_subcommand proxy; and __fish_seen_subcommand_from list" -s h -l help -d 'Print help'
complete -c hopash -n "__fish_hopash_using_subcommand proxy; and __fish_seen_subcommand_from select" -l json -d 'Emit a versioned JSON document or NDJSON stream'
complete -c hopash -n "__fish_hopash_using_subcommand proxy; and __fish_seen_subcommand_from select" -s h -l help -d 'Print help'
complete -c hopash -n "__fish_hopash_using_subcommand latency; and not __fish_seen_subcommand_from list show" -s h -l help -d 'Print help'
complete -c hopash -n "__fish_hopash_using_subcommand latency; and not __fish_seen_subcommand_from list show" -f -a "list" -d 'List latency samples for all Active Profile Nodes'
complete -c hopash -n "__fish_hopash_using_subcommand latency; and not __fish_seen_subcommand_from list show" -f -a "show" -d 'Show the latency sample for one Active Profile Node'
complete -c hopash -n "__fish_hopash_using_subcommand latency; and __fish_seen_subcommand_from list" -l json -d 'Emit a versioned JSON document or NDJSON stream'
complete -c hopash -n "__fish_hopash_using_subcommand latency; and __fish_seen_subcommand_from list" -s h -l help -d 'Print help'
complete -c hopash -n "__fish_hopash_using_subcommand latency; and __fish_seen_subcommand_from show" -l json -d 'Emit a versioned JSON document or NDJSON stream'
complete -c hopash -n "__fish_hopash_using_subcommand latency; and __fish_seen_subcommand_from show" -s h -l help -d 'Print help'
complete -c hopash -n "__fish_hopash_using_subcommand rule; and not __fish_seen_subcommand_from list add replace remove" -s h -l help -d 'Print help'
complete -c hopash -n "__fish_hopash_using_subcommand rule; and not __fish_seen_subcommand_from list add replace remove" -f -a "list" -d 'List Local Rule Set entries in effective order'
complete -c hopash -n "__fish_hopash_using_subcommand rule; and not __fish_seen_subcommand_from list add replace remove" -f -a "add" -d 'Insert one complete Rule String at an explicit position'
complete -c hopash -n "__fish_hopash_using_subcommand rule; and not __fish_seen_subcommand_from list add replace remove" -f -a "replace" -d 'Replace one exact, complete Rule String'
complete -c hopash -n "__fish_hopash_using_subcommand rule; and not __fish_seen_subcommand_from list add replace remove" -f -a "remove" -d 'Remove one exact, complete Rule String'
complete -c hopash -n "__fish_hopash_using_subcommand rule; and __fish_seen_subcommand_from list" -l json -d 'Emit a versioned JSON document or NDJSON stream'
complete -c hopash -n "__fish_hopash_using_subcommand rule; and __fish_seen_subcommand_from list" -s h -l help -d 'Print help'
complete -c hopash -n "__fish_hopash_using_subcommand rule; and __fish_seen_subcommand_from add" -l before -d 'Insert before this exact, complete Rule String' -r
complete -c hopash -n "__fish_hopash_using_subcommand rule; and __fish_seen_subcommand_from add" -l after -d 'Insert after this exact, complete Rule String' -r
complete -c hopash -n "__fish_hopash_using_subcommand rule; and __fish_seen_subcommand_from add" -l prepend -d 'Insert before every existing rule'
complete -c hopash -n "__fish_hopash_using_subcommand rule; and __fish_seen_subcommand_from add" -l append -d 'Insert after every existing rule'
complete -c hopash -n "__fish_hopash_using_subcommand rule; and __fish_seen_subcommand_from add" -l json -d 'Emit a versioned JSON document or NDJSON stream'
complete -c hopash -n "__fish_hopash_using_subcommand rule; and __fish_seen_subcommand_from add" -s h -l help -d 'Print help'
complete -c hopash -n "__fish_hopash_using_subcommand rule; and __fish_seen_subcommand_from replace" -l json -d 'Emit a versioned JSON document or NDJSON stream'
complete -c hopash -n "__fish_hopash_using_subcommand rule; and __fish_seen_subcommand_from replace" -s h -l help -d 'Print help'
complete -c hopash -n "__fish_hopash_using_subcommand rule; and __fish_seen_subcommand_from remove" -l json -d 'Emit a versioned JSON document or NDJSON stream'
complete -c hopash -n "__fish_hopash_using_subcommand rule; and __fish_seen_subcommand_from remove" -s h -l help -d 'Print help'
complete -c hopash -n "__fish_hopash_using_subcommand logs" -l follow -d 'Continue streaming until interrupted'
complete -c hopash -n "__fish_hopash_using_subcommand logs" -l json -d 'Emit a versioned JSON document or NDJSON stream'
complete -c hopash -n "__fish_hopash_using_subcommand logs" -s h -l help -d 'Print help'
complete -c hopash -n "__fish_hopash_using_subcommand help" -s h -l help -d 'Print help (see more with \'--help\')'
complete -c hopash -n "__fish_hopash_using_subcommand help" -f -a "agent" -d "Stable operation guidance for AI Agents and scripts"
