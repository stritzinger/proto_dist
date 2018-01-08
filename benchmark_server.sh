#!/bin/bash

SCRIPTNAME=$(basename $0)
SCRIPTPATH=$(readlink -- "$0" || echo "$0")
SCRIPTDIR=$(cd -- "$(dirname -- "$SCRIPTPATH")" && pwd)
COOKIE="f4efa7eb-a922-469d-8858-bb4c6b926574"
SNAME="server"

usage() {
  echo "USAGE: $SCRIPTNAME [-p DIST_PROTO]"
  echo "e.g.   $SCRIPTNAME -p proto_tcp"
  exit 1
}

error() {
  reason="$1"
  echo "ERROR: ${reason}" >&2
  usage
}

DIST_PROTO=""

while getopts "hs:p:" opt; do
  case ${opt} in
    p) DIST_PROTO=$OPTARG;;
    *) usage;;
  esac
done

shift $((OPTIND - 1))

if [ x"$@" != x ]; then
  error "unexpected parameters"
fi

if [ x"$DIST_PROTO" != x ]; then
  DIST_PROTO="-proto_dist $DIST_PROTO"
fi

erl -pa "$SCRIPTDIR/_build/default/lib/proto_dist/ebin" -noinput -sname "$SNAME" -setcookie "$COOKIE" $DIST_PROTO -s benchmark_server
