# bash completion for franta
_franta() {
    local cur prev words cword
    if declare -F _init_completion >/dev/null 2>&1; then
        _init_completion || return
    else
        cur="${COMP_WORDS[COMP_CWORD]}"
        prev="${COMP_WORDS[COMP_CWORD-1]}"
        words=("${COMP_WORDS[@]}")
        cword=$COMP_CWORD
    fi

    # Detect the subcommand (word 1), if any.
    local sub=""
    case "${words[1]}" in
        consume) sub="consume" ;;
        version) sub="version" ;;
    esac

    # Value completion driven by the preceding flag.
    case "$prev" in
        --config|-config)
            if declare -F _filedir >/dev/null 2>&1; then
                _filedir
            else
                mapfile -t COMPREPLY < <(compgen -f -- "$cur")
            fi
            return ;;
        --from|-from)
            mapfile -t COMPREPLY < <(compgen -W "end beginning last: 1h 30m" -- "$cur")
            return ;;
        --cluster|-cluster|--filter|-filter)
            return ;; # free-form value, no candidates
    esac

    # Flag-name completion.
    if [[ "$cur" == -* ]]; then
        local flags="--config --cluster --from"
        [[ "$sub" == "consume" ]] && flags="$flags --filter"
        mapfile -t COMPREPLY < <(compgen -W "$flags" -- "$cur")
        return
    fi

    # Position 1 with no subcommand yet: offer subcommands.
    if [[ -z "$sub" && $cword -eq 1 ]]; then
        mapfile -t COMPREPLY < <(compgen -W "version consume" -- "$cur")
        return
    fi
    # TOPIC positional: no candidates.
}
complete -F _franta franta
