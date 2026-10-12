#!/bin/bash
set -e

echo "=== Mooncake Harness Initialization ==="

echo "--- Checking build directory ---"
if [ ! -d "build" ]; then
  echo "WARNING: No build directory found. Run: mkdir build && cd build && cmake .. && make -j"
else
  echo "Build directory exists."
fi

echo "--- Checking code format (dry-run) ---"
if [ -f "scripts/code_format.sh" ]; then
  bash scripts/code_format.sh --check || echo "WARNING: Code format check found issues. Run scripts/code_format.sh to fix."
fi

echo "--- Checking metadata server ---"
if curl -s http://127.0.0.1:8080/metadata > /dev/null 2>&1; then
  echo "Metadata server is running on port 8080."
else
  echo "WARNING: Metadata server not detected. Start it: mooncake_http_metadata_server --port 8080 &"
  echo "Set MC_METADATA_SERVER=http://127.0.0.1:8080/metadata"
fi

echo "--- Checking C++ unit tests ---"
if [ -f "build/Makefile" ]; then
  echo "Build Makefile found. C++ tests can be run: cd build && MC_METADATA_SERVER=http://127.0.0.1:8080/metadata make test -j ARGS='-V'"
else
  echo "WARNING: Build not configured. Run cmake from build directory first."
fi

echo "--- Checking Python tests ---"
if [ -f "scripts/run_tests.sh" ]; then
  echo "Python test script available: ./scripts/run_tests.sh (requires metadata server)"
else
  echo "WARNING: Python test script not found."
fi

echo "=== Initialization Complete ==="
echo ""
echo "Next steps:"
echo "1. Read feature_list.json to see current feature state"
echo "2. Pick ONE unfinished feature to work on"
echo "3. Implement only that feature"
echo "4. Run verification: scripts/code_format.sh --check, cmake build, unit tests"
echo "5. Update progress.md with evidence before claiming done"
