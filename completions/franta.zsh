#compdef franta
# zsh completion for franta. Installed as an autoloaded function named
# `_franta` (the file body IS the completion function).

local -a common_flags
common_flags=(
  '--config[path to config file]:config file:_files'
  '--cluster[cluster name]:cluster name:'
  '--from[start position: end|beginning|last\:N|<duration>|RFC3339]:position:(end beginning last\: 1h 30m)'
)

# Position 1: subcommand, or a TUI flag / topic.
if (( CURRENT == 2 )); then
  _arguments \
    '1:command:((version\:"print version and exit" consume\:"stream records to stdout"))' \
    $common_flags \
    '*:topic:' && return
fi

case ${words[2]} in
  consume)
    _arguments \
      $common_flags \
      '--filter[DSL filter query]:filter:' \
      '*:topic:' ;;
  version)
    _arguments ;;
  *)
    _arguments $common_flags '*:topic:' ;;
esac
