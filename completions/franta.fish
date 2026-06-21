# fish completion for franta

# Subcommands (only when no subcommand has been given yet).
complete -c franta -n __fish_use_subcommand -a version -d 'Print version and exit'
complete -c franta -n __fish_use_subcommand -a consume -d 'Stream records to stdout'

# Flags available in every context (TUI default + consume).
complete -c franta -l config -r -d 'Path to config file'
complete -c franta -l cluster -x -d 'Cluster name'
complete -c franta -l from -x -d 'Start position' -a 'end beginning last: 1h 30m'

# consume-only flag.
complete -c franta -n '__fish_seen_subcommand_from consume' -l filter -x -d 'DSL filter query'
