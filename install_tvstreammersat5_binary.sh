#!/usr/bin/env bash
set -Eeuo pipefail

# TVStreammerSAT5 binary installer for Ubuntu/Debian.
# Run this script from a folder that contains either:
#   TVStreammerSAT5
#   tvstreammersat5-ca-newcamd.so                  (optional)
#   oscam-mini/oscam-mini                         (optional)
# or from the project root after a build, where those files live under build/.
#
# The installer intentionally preserves existing configuration/data files.
# It records packages newly installed by this run so uninstall_tvstreammersat5.sh
# can remove only installer-added dependencies instead of purging arbitrary
# pre-existing system packages.

APP_NAME="TVStreammerSAT5"
SERVICE_NAME="tvstreammersat5.service"
OSCAM_SERVICE_NAME="oscam-mini.service"
INSTALL_DIR="${TVS_INSTALL_DIR:-/opt/TVStreammerSAT5}"
STATE_DIR="${TVS_INSTALLER_STATE_DIR:-/var/lib/tvstreammersat5-installer}"
SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
START_SERVICE=1
DRY_RUN=0

usage() {
    cat <<'USAGE'
Usage: sudo ./install_tvstreammersat5_binary.sh [options]

Options:
  --source DIR      Folder containing binaries, or project root with build/
  --install-dir DIR Application directory (default: /opt/TVStreammerSAT5)
  --no-start        Install but do not start services
  --dry-run         Show what would be done without changing the system
  -h, --help        Show this help
USAGE
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --source)
            [[ $# -ge 2 ]] || { echo "--source requires a directory" >&2; exit 2; }
            SOURCE_DIR="$(cd "$2" && pwd)"
            shift 2
            ;;
        --install-dir)
            [[ $# -ge 2 ]] || { echo "--install-dir requires a directory" >&2; exit 2; }
            INSTALL_DIR="$2"
            shift 2
            ;;
        --no-start)
            START_SERVICE=0
            shift
            ;;
        --dry-run)
            DRY_RUN=1
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1" >&2
            usage >&2
            exit 2
            ;;
    esac
done

if [[ "${EUID}" -ne 0 ]]; then
    if command -v sudo >/dev/null 2>&1; then
        args=(--source "$SOURCE_DIR" --install-dir "$INSTALL_DIR")
        [[ "$START_SERVICE" -eq 0 ]] && args+=(--no-start)
        [[ "$DRY_RUN" -eq 1 ]] && args+=(--dry-run)
        exec sudo -E bash "$0" "${args[@]}"
    fi
    echo "Run as root or install sudo first." >&2
    exit 1
fi

if ! command -v apt-get >/dev/null 2>&1 || ! command -v dpkg-query >/dev/null 2>&1; then
    echo "This installer requires an apt-based Ubuntu/Debian system." >&2
    exit 1
fi

run() {
    if [[ "$DRY_RUN" -eq 1 ]]; then
        printf '+ '
        printf '%q ' "$@"
        printf '\n'
    else
        "$@"
    fi
}

first_existing() {
    local path
    for path in "$@"; do
        if [[ -f "$path" ]]; then
            printf '%s\n' "$path"
            return 0
        fi
    done
    return 1
}

first_existing_dir() {
    local path
    for path in "$@"; do
        if [[ -d "$path" ]]; then
            printf '%s\n' "$path"
            return 0
        fi
    done
    return 1
}

MAIN_BINARY="$(first_existing \
    "$SOURCE_DIR/TVStreammerSAT5" \
    "$SOURCE_DIR/build/TVStreammerSAT5" || true)"
CA_PLUGIN="$(first_existing \
    "$SOURCE_DIR/tvstreammersat5-ca-newcamd.so" \
    "$SOURCE_DIR/build/tvstreammersat5-ca-newcamd.so" || true)"
OSCAM_BINARY="$(first_existing \
    "$SOURCE_DIR/oscam-mini/oscam-mini" \
    "$SOURCE_DIR/build/oscam-mini/oscam-mini" \
    "$SOURCE_DIR/oscam-mini" || true)"
OSCAM_SERVICE="$(first_existing \
    "$SOURCE_DIR/oscam-mini/oscam-mini.service" \
    "$SOURCE_DIR/packaging/oscam-mini/oscam-mini.service" \
    "$SOURCE_DIR/oscam-mini.service" || true)"
OSCAM_DEFAULT_CONFIG="$(first_existing_dir \
    "$SOURCE_DIR/oscam-mini/default-config" \
    "$SOURCE_DIR/packaging/oscam-mini/default-config" \
    "$SOURCE_DIR/default-config" || true)"

if [[ -z "$MAIN_BINARY" ]]; then
    cat >&2 <<EOFERR
TVStreammerSAT5 binary not found in:
  $SOURCE_DIR/TVStreammerSAT5
  $SOURCE_DIR/build/TVStreammerSAT5

Build first with:
  cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
  cmake --build build --parallel "\$(nproc)"
EOFERR
    exit 1
fi

if command -v file >/dev/null 2>&1; then
    if ! file "$MAIN_BINARY" | grep -q 'ELF'; then
        echo "Main binary is not an ELF executable: $MAIN_BINARY" >&2
        exit 1
    fi
fi

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT
BEFORE_PACKAGES="$TMP_DIR/packages.before"
AFTER_PACKAGES="$TMP_DIR/packages.after"
NEW_PACKAGES="$TMP_DIR/packages.new"

dpkg-query -W -f='${binary:Package}\n' 2>/dev/null | sort -u > "$BEFORE_PACKAGES"

# Runtime stack required by the current TVStreammerSAT5 binary and its external
# GStreamer transcoder process. Development package names are deliberately used
# for a few libraries because they are stable across Ubuntu 22.04/24.04 and pull
# the matching versioned runtime library automatically.
DEPENDENCIES=(
    ca-certificates
    libcurl4-openssl-dev
    libjsoncpp-dev
    libssl-dev
    libcrypt-dev
    libdvbcsa-dev
    libboost-thread-dev
    gstreamer1.0-tools
    gstreamer1.0-plugins-base
    gstreamer1.0-plugins-good
    gstreamer1.0-plugins-bad
    gstreamer1.0-plugins-ugly
    gstreamer1.0-libav
    gstreamer1.0-rtsp
    gstreamer1.0-vaapi
    vainfo
    intel-media-va-driver
)

printf 'Source directory : %s\n' "$SOURCE_DIR"
printf 'Install directory: %s\n' "$INSTALL_DIR"
printf 'Main binary      : %s\n' "$MAIN_BINARY"
printf 'CA plugin        : %s\n' "${CA_PLUGIN:-not found / skipped}"
printf 'OSCam-mini       : %s\n' "${OSCAM_BINARY:-not found / skipped}"

echo "Installing runtime dependencies..."
run apt-get update
run env DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends "${DEPENDENCIES[@]}"

if [[ "$DRY_RUN" -eq 0 ]]; then
    dpkg-query -W -f='${binary:Package}\n' 2>/dev/null | sort -u > "$AFTER_PACKAGES"
    comm -13 "$BEFORE_PACKAGES" "$AFTER_PACKAGES" > "$NEW_PACKAGES"
    mkdir -p "$STATE_DIR"
    cp "$NEW_PACKAGES" "$STATE_DIR/installed-packages.txt"
    printf '%s\n' "$INSTALL_DIR" > "$STATE_DIR/install-dir.txt"
    printf '%s\n' "$SOURCE_DIR" > "$STATE_DIR/source-dir.txt"
fi

# Stop only the services that are about to be replaced. Existing user data in
# INSTALL_DIR is left untouched.
if systemctl list-unit-files "$SERVICE_NAME" >/dev/null 2>&1; then
    run systemctl stop "$SERVICE_NAME" || true
fi
if [[ -n "$OSCAM_BINARY" ]] && systemctl list-unit-files "$OSCAM_SERVICE_NAME" >/dev/null 2>&1; then
    run systemctl stop "$OSCAM_SERVICE_NAME" || true
fi

run install -d -m 0755 "$INSTALL_DIR"
run install -m 0755 "$MAIN_BINARY" "$INSTALL_DIR/$APP_NAME"

if [[ -n "$CA_PLUGIN" ]]; then
    run install -d -m 0755 "$INSTALL_DIR/ca-plugins"
    run install -m 0755 "$CA_PLUGIN" "$INSTALL_DIR/ca-plugins/tvstreammersat5-ca-newcamd.so"
fi

if [[ -n "$OSCAM_BINARY" ]]; then
    run install -d -m 0755 "$INSTALL_DIR/oscam-mini"
    run install -m 0755 "$OSCAM_BINARY" "$INSTALL_DIR/oscam-mini/oscam-mini"
    run install -d -m 0755 "$INSTALL_DIR/oscam-mini/config"

    if [[ -n "$OSCAM_DEFAULT_CONFIG" ]]; then
        run install -d -m 0755 "$INSTALL_DIR/oscam-mini/default-config"
        if [[ "$DRY_RUN" -eq 0 ]]; then
            cp -a "$OSCAM_DEFAULT_CONFIG/." "$INSTALL_DIR/oscam-mini/default-config/"
            for cfg in oscam.conf oscam.server oscam.user; do
                if [[ -f "$OSCAM_DEFAULT_CONFIG/$cfg" && ! -e "$INSTALL_DIR/oscam-mini/config/$cfg" ]]; then
                    cp -a "$OSCAM_DEFAULT_CONFIG/$cfg" "$INSTALL_DIR/oscam-mini/config/$cfg"
                fi
            done
        else
            echo "+ copy OSCam default-config and seed missing config files"
        fi
    fi

    if [[ -n "$OSCAM_SERVICE" ]]; then
        run install -m 0644 "$OSCAM_SERVICE" "/etc/systemd/system/$OSCAM_SERVICE_NAME"
    else
        if [[ "$DRY_RUN" -eq 0 ]]; then
            cat > "/etc/systemd/system/$OSCAM_SERVICE_NAME" <<EOFUNIT
[Unit]
Description=TVStreammerSAT5 OSCam-mini Newcamd/Phoenix server
After=network.target
Conflicts=oscam.service

[Service]
Type=simple
User=root
Group=root
WorkingDirectory=$INSTALL_DIR/oscam-mini
ExecStart=$INSTALL_DIR/oscam-mini/oscam-mini -c $INSTALL_DIR/oscam-mini/config
SuccessExitStatus=15 SIGTERM
Restart=on-failure
RestartSec=2
Nice=5
LimitNOFILE=1024

[Install]
WantedBy=multi-user.target
EOFUNIT
        else
            echo "+ create /etc/systemd/system/$OSCAM_SERVICE_NAME"
        fi
    fi
fi

# Preserve a customized existing main unit. On a clean host create the standard
# service expected by the project and by the in-process restart command.
if [[ ! -f "/etc/systemd/system/$SERVICE_NAME" ]]; then
    if [[ "$DRY_RUN" -eq 0 ]]; then
        cat > "/etc/systemd/system/$SERVICE_NAME" <<EOFUNIT
[Unit]
Description=TVStreammerSAT5 IPTV/DVB streaming server
Wants=network-online.target
After=network-online.target

[Service]
Type=simple
User=root
Group=root
WorkingDirectory=$INSTALL_DIR
ExecStart=$INSTALL_DIR/$APP_NAME
Restart=on-failure
RestartSec=3
TimeoutStopSec=35
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
EOFUNIT
    else
        echo "+ create /etc/systemd/system/$SERVICE_NAME"
    fi
else
    echo "Existing $SERVICE_NAME preserved."
fi

run systemctl daemon-reload
run systemctl enable "$SERVICE_NAME"
if [[ -n "$OSCAM_BINARY" ]]; then
    # Avoid two OSCam instances fighting for the same reader/port.
    if systemctl list-unit-files oscam.service >/dev/null 2>&1; then
        run systemctl disable --now oscam.service || true
    fi
    run systemctl enable "$OSCAM_SERVICE_NAME"
fi

# Verify dynamic linker dependencies after package installation.
if [[ "$DRY_RUN" -eq 0 ]]; then
    MISSING=0
    for elf in "$INSTALL_DIR/$APP_NAME" \
               "$INSTALL_DIR/ca-plugins/tvstreammersat5-ca-newcamd.so" \
               "$INSTALL_DIR/oscam-mini/oscam-mini"; do
        [[ -f "$elf" ]] || continue
        if ldd "$elf" 2>/dev/null | grep -q 'not found'; then
            echo "Missing shared libraries for $elf:" >&2
            ldd "$elf" | grep 'not found' >&2 || true
            MISSING=1
        fi
    done
    if [[ "$MISSING" -ne 0 ]]; then
        echo "Installation stopped before service start because shared libraries are missing." >&2
        exit 1
    fi

    REQUIRED_GST_ELEMENTS=(
        udpsrc udpsink rtpmp2tpay srtsrc srtsink
        tcpserversink hlssink hlsdemux tsparse tsdemux mpegtsmux
        avdec_h264 deinterlace videoconvert videoscale watchdog
    )
    MISSING_GST=()
    for element in "${REQUIRED_GST_ELEMENTS[@]}"; do
        gst-inspect-1.0 "$element" >/dev/null 2>&1 || MISSING_GST+=("$element")
    done
    if [[ "${#MISSING_GST[@]}" -gt 0 ]]; then
        printf 'Warning: missing GStreamer elements: %s\n' "${MISSING_GST[*]}" >&2
        echo "Some protocols/transcoding functions may be unavailable." >&2
    fi
    if gst-inspect-1.0 nvh264enc >/dev/null 2>&1; then
        echo "NVIDIA NVENC detected: nvh264enc"
    elif gst-inspect-1.0 x264enc >/dev/null 2>&1; then
        echo "NVENC is not available; H.264 transcoding will fall back to CPU x264enc."
    else
        echo "Warning: neither nvh264enc nor x264enc is available; H.264 transcoding is unavailable." >&2
    fi
fi

if [[ "$START_SERVICE" -eq 1 ]]; then
    if [[ -n "$OSCAM_BINARY" ]]; then
        run systemctl restart "$OSCAM_SERVICE_NAME"
    fi
    run systemctl restart "$SERVICE_NAME"
fi

if [[ "$DRY_RUN" -eq 0 ]]; then
    VERSION="$(strings "$INSTALL_DIR/$APP_NAME" 2>/dev/null | grep -E '^202\.[0-9]+$' | tail -1 || true)"
    echo
    echo "TVStreammerSAT5 installation complete."
    echo "Installed binary: $INSTALL_DIR/$APP_NAME"
    [[ -n "$VERSION" ]] && echo "Detected version : $VERSION"
    echo "Service status   : systemctl status $SERVICE_NAME --no-pager"
    echo "Journal          : journalctl -u $SERVICE_NAME -f"
    echo "Uninstall        : sudo ./uninstall_tvstreammersat5.sh"
else
    echo "Dry run complete; no changes were made."
fi
