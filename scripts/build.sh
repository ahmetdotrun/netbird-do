#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
project_dir=$PWD
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/netbird-do-build.XXXXXX")
trap 'rm -rf "$work_dir"' EXIT HUP INT TERM
export GOWORK=off

mkdir -p "$work_dir/engine" src test/websocket-error test/bridge
cp go/go.mod go/go.sum go/*.go "$work_dir/engine/"
cd "$work_dir/engine"
go mod download

# Patch private copies, leaving the module cache and source go.mod untouched.
for module in netbird websocket; do
  case "$module" in
    netbird) module_path=github.com/netbirdio/netbird ;;
    websocket) module_path=github.com/coder/websocket ;;
  esac
  upstream_dir=$(go list -m -f '{{.Dir}}' "$module_path")
  cp -R "$upstream_dir" "$work_dir/$module"
  chmod -R u+w "$work_dir/$module"
  git -C "$work_dir/$module" apply "$project_dir/patches/$module/"*.patch
  go mod edit "-replace=$module_path=$work_dir/$module"
done

GOOS=js GOARCH=wasm CGO_ENABLED=0 go build -p 4 -mod=readonly -trimpath -ldflags='-s -w' -o "$project_dir/src/netbird.wasm" .
cp "$(go env GOROOT)/lib/wasm/wasm_exec.js" "$project_dir/src/wasm_exec.js"
GOOS=js GOARCH=wasm CGO_ENABLED=0 go list -deps -json . | go run "$project_dir/scripts/notices.go" > "$project_dir/src/THIRD_PARTY_NOTICES.txt"
if [ "${1:-}" = "--tests" ]; then
  GOOS=js GOARCH=wasm CGO_ENABLED=0 go build -p 4 -mod=readonly -trimpath -ldflags='-s -w' -o "$project_dir/test/websocket-error/engine.wasm" "$project_dir/test/websocket-error/main.go"
  GOOS=js GOARCH=wasm CGO_ENABLED=0 go test -c -p 4 -mod=readonly -trimpath -ldflags='-s -w' -o "$project_dir/test/bridge/engine.wasm" .
fi
printf 'Built the NetBird WASM engine.\n'
