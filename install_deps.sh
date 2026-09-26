#!/usr/bin/env bash
set -euo pipefail

# TVStreammerSAT5 host build/runtime dependencies for Ubuntu/Debian.
# This script intentionally installs only libraries used by the current CMake
# target plus GStreamer runtime plugins used by the protocol/transcoder modules.

if ! command -v apt-get >/dev/null 2>&1; then
    echo "This installer requires an apt-based Ubuntu/Debian system." >&2
    exit 1
fi

SUDO=()
if [[ "${EUID}" -ne 0 ]]; then
    if ! command -v sudo >/dev/null 2>&1; then
        echo "Run as root or install sudo first." >&2
        exit 1
    fi
    SUDO=(sudo)
fi

APT_GET=("${SUDO[@]}" apt-get)
BOOST_SYSTEM_DEV_PACKAGE="libboost-dev"
if apt-cache show libboost-system-dev >/dev/null 2>&1; then
    BOOST_SYSTEM_DEV_PACKAGE="libboost-system-dev"
fi
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "Installing TVStreammerSAT5 dependencies..."

"${APT_GET[@]}" update
"${APT_GET[@]}" install -y --no-install-recommends \
    build-essential \
    cmake \
    pkg-config \
    libgstreamer1.0-dev \
    libgstreamer-plugins-base1.0-dev \
    libgstreamer-plugins-bad1.0-dev \
    libcurl4-openssl-dev \
    libjsoncpp-dev \
    libssl-dev \
    libcrypt-dev \
    libdvbcsa-dev \
    "${BOOST_SYSTEM_DEV_PACKAGE}" \
    libboost-thread-dev \
    gstreamer1.0-tools \
    gstreamer1.0-plugins-base \
    gstreamer1.0-plugins-good \
    gstreamer1.0-plugins-bad \
    gstreamer1.0-plugins-ugly \
    gstreamer1.0-libav \
    gstreamer1.0-rtsp \
    gstreamer1.0-vaapi \
    vainfo \
    intel-media-va-driver \
    ca-certificates

"${APT_GET[@]}" clean

if [[ -x "${ROOT_DIR}/scripts/check_transcoder_plugins.sh" ]]; then
    "${ROOT_DIR}/scripts/check_transcoder_plugins.sh"
fi

echo "Dependencies installed."
echo "Build with: cmake -S . -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build --parallel"
