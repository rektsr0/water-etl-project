#!/bin/sh
set -e
# Resolve JAVA_HOME when the default path does not exist (e.g. arm64 vs amd64 OpenJDK packages).
if [ ! -d "${JAVA_HOME:-}" ]; then
  JAVA_BIN="$(command -v java)"
  export JAVA_HOME="$(dirname "$(dirname "$(readlink -f "$JAVA_BIN")")")"
fi
exec "$@"
