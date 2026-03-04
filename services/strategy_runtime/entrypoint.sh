#!/bin/bash
# Entrypoint for strategy_runtime container.
# Compiles the C++ engine if KIRA_CPP_ENGINE=true.
# Always recompiles if source files are newer than the existing .so.

set -e

if [ "${KIRA_CPP_ENGINE}" = "true" ] && [ -d "/app/cpp" ]; then
    SO_FILE=$(find /app -maxdepth 1 -name "kira_engine*.so" 2>/dev/null | head -1)
    NEEDS_BUILD=false

    if [ -z "$SO_FILE" ]; then
        NEEDS_BUILD=true
    else
        # Recompile if any source file is newer than the .so
        NEWER=$(find /app/cpp -name "*.h" -o -name "*.cpp" -o -name "CMakeLists.txt" | \
                xargs -I{} find {} -newer "$SO_FILE" 2>/dev/null | head -1)
        if [ -n "$NEWER" ]; then
            NEEDS_BUILD=true
            echo "🔄 C++ source changed, recompiling..."
        fi
    fi

    if [ "$NEEDS_BUILD" = true ]; then
        echo "🔧 Building C++ engine (kira_engine)..."
        cd /app/cpp
        rm -rf build
        mkdir -p build && cd build
        cmake -DCMAKE_BUILD_TYPE=Release \
              -Dpybind11_DIR=$(python3 -c "import pybind11; print(pybind11.get_cmake_dir())") \
              .. 2>&1 | tail -3
        make -j$(nproc) 2>&1 | tail -5
        cp kira_engine*.so /app/
        echo "✅ C++ engine built successfully"
    else
        echo "✅ C++ engine up-to-date: $SO_FILE"
    fi
fi

exec "$@"

