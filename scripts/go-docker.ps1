# Run any Go command inside the official Go container, for machines without
# a local Go toolchain. Module and build caches persist in Docker volumes.
#
#   .\scripts\go-docker.ps1 test -race ./...
#   .\scripts\go-docker.ps1 run ./cmd/raftkv-bench -mode all
param([Parameter(ValueFromRemainingArguments = $true)] [string[]] $GoArgs)

$root = Split-Path -Parent $PSScriptRoot
docker run --rm `
    -v "${root}:/src" -w /src `
    -v raftkv-gomod:/go/pkg/mod `
    -v raftkv-gocache:/root/.cache/go-build `
    golang:1.23 go @GoArgs
exit $LASTEXITCODE
