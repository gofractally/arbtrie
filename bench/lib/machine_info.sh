#!/bin/bash
# Capture machine and build metadata as JSON.
# Usage: bash bench/lib/machine_info.sh [BUILD_DIR]
# Writes to stdout.
set -euo pipefail

BUILD_DIR="${1:-build/release}"
REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

# CPU info (Linux vs macOS)
if [ -f /proc/cpuinfo ]; then
    CPU=$(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2 | sed 's/^ //')
    CORES=$(nproc 2>/dev/null || echo "?")
elif command -v sysctl &>/dev/null; then
    CPU=$(sysctl -n machdep.cpu.brand_string 2>/dev/null || echo "unknown")
    CORES=$(sysctl -n hw.ncpu 2>/dev/null || echo "?")
else
    CPU="unknown"
    CORES="?"
fi

# RAM (Linux vs macOS)
if [ -f /proc/meminfo ]; then
    RAM_KB=$(grep MemTotal /proc/meminfo | awk '{print $2}')
    RAM_GB=$(( RAM_KB / 1024 / 1024 ))
elif command -v sysctl &>/dev/null; then
    RAM_BYTES=$(sysctl -n hw.memsize 2>/dev/null || echo 0)
    RAM_GB=$(( RAM_BYTES / 1024 / 1024 / 1024 ))
else
    RAM_GB=0
fi

# OS
OS=$(uname -srm)

# Filesystem (Linux)
if command -v df &>/dev/null && command -v awk &>/dev/null; then
    FS=$(df -T "$REPO_ROOT" 2>/dev/null | awk 'NR==2{print $2}' || echo "unknown")
else
    FS="unknown"
fi

PAGE_SIZE=$(getconf PAGESIZE 2>/dev/null || echo "4096")

# Git info
GIT_COMMIT=$(cd "$REPO_ROOT" && git rev-parse --short HEAD 2>/dev/null || echo "unknown")
GIT_BRANCH=$(cd "$REPO_ROOT" && git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "unknown")
GIT_DIRTY=$(cd "$REPO_ROOT" && git diff --quiet 2>/dev/null && echo "false" || echo "true")

# Compiler (check what the build used)
COMPILER="unknown"
if [ -f "$REPO_ROOT/$BUILD_DIR/CMakeCache.txt" ]; then
    COMPILER=$(grep 'CMAKE_CXX_COMPILER:' "$REPO_ROOT/$BUILD_DIR/CMakeCache.txt" 2>/dev/null \
        | head -1 | sed 's/.*=//' | xargs basename 2>/dev/null || echo "unknown")
fi

# Build type
BUILD_TYPE="unknown"
if [ -f "$REPO_ROOT/$BUILD_DIR/CMakeCache.txt" ]; then
    BUILD_TYPE=$(grep 'CMAKE_BUILD_TYPE:' "$REPO_ROOT/$BUILD_DIR/CMakeCache.txt" 2>/dev/null \
        | head -1 | sed 's/.*=//' || echo "unknown")
fi

DATE=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

cat <<EOF
{
  "cpu": "$CPU",
  "cores": $CORES,
  "ram_gb": $RAM_GB,
  "os": "$OS",
  "filesystem": "$FS",
  "page_size": $PAGE_SIZE,
  "git_commit": "$GIT_COMMIT",
  "git_branch": "$GIT_BRANCH",
  "git_dirty": $GIT_DIRTY,
  "compiler": "$COMPILER",
  "build_type": "$BUILD_TYPE",
  "date": "$DATE"
}
EOF
