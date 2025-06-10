#!/usr/bin/env bash

set -euo pipefail

cd -P -- "$(dirname -- "$0")/.."

function help() {
  echo "Usage: $0 check|fix"
  exit 1
}

if [[ "$#" -lt 1 ]]; then
  help
fi

function fix() {
  find . \( -name '*.app.src' \
       -o -name '*.erl' \
       -o -name '*.hrl' \
       -o -name 'rebar.config' \) \
       -not -path '*/_build/*' \
       -not -path '*/deps/*' \
       -not -path '*/_checkouts/*' \
       -type f \
       | xargs scripts/erlfmt -w
}

function check() {
  fix

  DIFF_FILES="$(git diff --name-only)"
  if [ "$DIFF_FILES" != '' ]; then
      echo "ERROR: Files below need reformatting"
      echo "$DIFF_FILES"
      exit 1
  fi
}

case $1 in
  check)
    check
    ;;
  fix)
    fix
    ;;
  *)
    help
    ;;
esac
