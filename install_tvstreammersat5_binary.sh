#!/usr/bin/env bash
# TVStreammerSAT5 203.68 — safe binary + web installer / updater for Ubuntu/Debian.
set -Eeuo pipefail
umask 022

APP=TVStreammerSAT5
UNIT=tvstreammersat5.service
DEFAULT_INSTALL_DIR=/opt/TVStreammerSAT5
SOURCE_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
INSTALL_DIR="${TVS_INSTALL_DIR:-}"
INSTALL_DIR_EXPLICIT=0
[[ -n "$INSTALL_DIR" ]] && INSTALL_DIR_EXPLICIT=1
BUILD_DIR=''
WEB_DIR=''
MODE=''
RESTART=''
DRY_RUN=0
INSTALL_DEPS=''
WITH_OSCAM=0

usage() {
    cat <<'EOF'
TVStreammerSAT5 installer / updater (binary + web + optional CA plugin).
Usage: sudo bash install_tvstreammersat5_binary_20368.sh [options]

  --source DIR       Project root containing web/ and a compiled binary
  --build-dir DIR    Exact CMake build directory (recommended for updates)
  --web-dir DIR      Exact directory containing preview/ and vendor/
  --install-dir DIR  Installation directory; existing systemd WorkingDirectory
                     is detected when possible (fallback /opt/TVStreammerSAT5)
  --mode install|update    Skip the interactive mode selection
  --restart         Start/restart the main service AFTER updating (interrupts streams)
  --no-restart      Deploy files WITHOUT restarting the service
  --install-deps    Install Ubuntu/Debian packages (new installs only by default)
  --skip-deps       Do not run apt (update default)
  --with-oscam      Also install supplied oscam-mini executable, if found
  --dry-run         Preview decisions and source paths; NO system changes
  --no-start        Alias for --no-restart (compatibility)
  -h, --help        Show this help

WARNING: Updating an active service requires a maintenance window. The script
never stops/restarts it without explicit confirmation or --restart.
Existing channel configuration, databases, keys and customized systemd units
are not overwritten. Backups of overwritten application files are retained.
EOF
}

while (($#)); do
    case "$1" in
        --source|--build-dir|--web-dir|--install-dir|--mode)
            (($# >= 2)) || { echo "ERROR: $1 requires a value" >&2; exit 2; }
            case "$1" in
                --source) SOURCE_DIR="$2" ;;
                --build-dir) BUILD_DIR="$2" ;;
                --web-dir) WEB_DIR="$2" ;;
                --install-dir) INSTALL_DIR="$2"; INSTALL_DIR_EXPLICIT=1 ;;
                --mode) MODE="$2" ;;
            esac
            shift 2 ;;
        --restart) RESTART=yes; shift ;;
        --no-restart|--no-start) RESTART=no; shift ;;
        --install-deps) INSTALL_DEPS=yes; shift ;;
        --skip-deps) INSTALL_DEPS=no; shift ;;
        --with-oscam) WITH_OSCAM=1; shift ;;
        --dry-run) DRY_RUN=1; shift ;;
        -h|--help) usage; exit 0 ;;
        *) echo "ERROR: unknown option $1" >&2; usage >&2; exit 2 ;;
    esac
done

fail() { echo "ERROR: $*" >&2; exit 1; }
log() { printf '\n==> %s\n' "$*"; }
ask_yes() {
    local answer
    [[ -t 0 ]] || fail 'A confirmation is required. Run interactively or provide --restart / --no-restart.'
    read -r -p "$1 [y/N]: " answer
    case "$answer" in y|Y|yes|YES|Yes|д|Д|да|Да|ДА) return 0 ;; *) return 1 ;; esac
}

[[ "$MODE" == '' || "$MODE" == install || "$MODE" == update ]] || fail 'Use --mode install or --mode update.'
[[ -d "$SOURCE_DIR" ]] || fail "Source directory does not exist: $SOURCE_DIR"
SOURCE_DIR="$(cd -- "$SOURCE_DIR" && pwd -P)"

# Existing customized service files are not changed. Detect /opt/tvstreammersat5
# instead of accidentally installing to a different case-sensitive /opt path.
if (( ! INSTALL_DIR_EXPLICIT )) && command -v systemctl >/dev/null 2>&1; then
    detected="$(systemctl show "$UNIT" -p WorkingDirectory --value 2>/dev/null || true)"
    if [[ -n "$detected" && -f "$detected/$APP" ]]; then
        INSTALL_DIR="$detected"
    fi
fi
INSTALL_DIR="${INSTALL_DIR:-$DEFAULT_INSTALL_DIR}"
[[ "$INSTALL_DIR" == /* && "$INSTALL_DIR" != / ]] || fail 'Installation directory must be an absolute path other than /.'
if [[ -e "$INSTALL_DIR" ]]; then
    [[ -d "$INSTALL_DIR" && ! -L "$INSTALL_DIR" ]] || fail 'Installation directory must be a real directory, not a symlink.'
    INSTALL_DIR="$(cd -- "$INSTALL_DIR" && pwd -P)"
fi

existing=no
[[ -f "$INSTALL_DIR/$APP" ]] && existing=yes
if [[ -z "$MODE" ]]; then
    [[ -t 0 ]] || fail 'Specify --mode install or --mode update when running non-interactively.'
    printf '\nChoose operation:\n  1) New installation\n  2) Update existing installation\n  0) Cancel\n'
    read -r -p 'Enter 1, 2 or 0: ' selection
    case "$selection" in 1) MODE=install ;; 2) MODE=update ;; *) fail 'Cancelled.' ;; esac
fi
if [[ "$MODE" == update && "$existing" != yes ]]; then
    fail "No existing executable found at $INSTALL_DIR/$APP; choose the correct --install-dir."
fi
if [[ "$MODE" == install && "$existing" == yes ]]; then
    fail "Existing installation found at $INSTALL_DIR; choose update to preserve it."
fi

# Avoid selecting an unrelated/stale build just because it happens to exist.
if [[ -n "$BUILD_DIR" ]]; then
    [[ "$BUILD_DIR" == /* ]] || BUILD_DIR="$SOURCE_DIR/$BUILD_DIR"
    [[ -d "$BUILD_DIR" ]] || fail "Build directory does not exist: $BUILD_DIR"
    BINARY="$BUILD_DIR/$APP"
    PLUGIN="$BUILD_DIR/tvstreammersat5-ca-newcamd.so"
elif [[ -f "$SOURCE_DIR/$APP" ]]; then
    BINARY="$SOURCE_DIR/$APP"
    PLUGIN="$SOURCE_DIR/tvstreammersat5-ca-newcamd.so"
elif [[ -f "$SOURCE_DIR/build/$APP" ]]; then
    BINARY="$SOURCE_DIR/build/$APP"
    PLUGIN="$SOURCE_DIR/build/tvstreammersat5-ca-newcamd.so"
else
    fail "No executable in $SOURCE_DIR or its build/ directory. Specify --build-dir build-preview-20368-fixed."
fi
[[ -f "$BINARY" && -s "$BINARY" ]] || fail "Compiled executable missing or empty: $BINARY"
[[ -x "$BINARY" ]] || fail "Binary is not executable: $BINARY"
if command -v file >/dev/null 2>&1; then
    file -b "$BINARY" | grep -q ELF || fail "Not an ELF binary: $BINARY"
fi
[[ -f "$PLUGIN" && -s "$PLUGIN" ]] || PLUGIN=''

[[ -n "$WEB_DIR" ]] || WEB_DIR="$SOURCE_DIR/web"
[[ "$WEB_DIR" == /* ]] || WEB_DIR="$SOURCE_DIR/$WEB_DIR"
[[ -d "$WEB_DIR" ]] || fail "web directory missing: $WEB_DIR"
[[ -s "$WEB_DIR/preview/preview-player.js" && -s "$WEB_DIR/preview/preview-player.css" ]] || \
    fail "Preview assets are missing from $WEB_DIR/preview"

# A source ZIP may contain only vendor/README.md. Preserve the already-installed
# mpegts.min.js on update; do not deploy a preview that cannot play HTTP MPEG-TS.
MPEGTS_SOURCE=''
if [[ -s "$WEB_DIR/vendor/mpegts.min.js" ]]; then
    MPEGTS_SOURCE="$WEB_DIR/vendor/mpegts.min.js"
elif [[ -s "$INSTALL_DIR/web/vendor/mpegts.min.js" ]]; then
    MPEGTS_SOURCE="$INSTALL_DIR/web/vendor/mpegts.min.js"
else
    fail "web/vendor/mpegts.min.js missing in source AND installed web. Fetch it first with: bash scripts/vendor_preview_libs.sh"
fi
[[ "$(wc -c < "$MPEGTS_SOURCE")" -ge 100000 ]] || fail "mpegts.min.js is unexpectedly small: $MPEGTS_SOURCE"

OSCAM_BINARY=''
if (( WITH_OSCAM )); then
    for candidate in "$SOURCE_DIR/oscam-mini/oscam-mini" "$SOURCE_DIR/build/oscam-mini/oscam-mini" "$BUILD_DIR/oscam-mini/oscam-mini"; do
        if [[ -f "$candidate" && -x "$candidate" ]]; then OSCAM_BINARY="$candidate"; break; fi
    done
    [[ -n "$OSCAM_BINARY" ]] || fail '--with-oscam requested but no compiled oscam-mini executable was found.'
fi

if [[ -z "$RESTART" ]]; then
    if [[ "$MODE" == update ]]; then
        echo 'Restarting the running service interrupts ALL currently streaming channels.'
        if ask_yes 'Restart TVStreammerSAT5 after deployment?'; then RESTART=yes; else RESTART=no; fi
    else
        if ask_yes 'Start TVStreammerSAT5 after installation?'; then RESTART=yes; else RESTART=no; fi
    fi
fi
if [[ -z "$INSTALL_DEPS" ]]; then
    if [[ "$MODE" == install ]]; then
        if ask_yes 'Install required Ubuntu/Debian packages with apt?'; then INSTALL_DEPS=yes; else INSTALL_DEPS=no; fi
    else INSTALL_DEPS=no; fi
fi
[[ "$MODE" == install || "$INSTALL_DEPS" == no ]] || \
    fail 'Package installation during update is disabled by default; run apt separately if necessary.'

active=no
if command -v systemctl >/dev/null 2>&1 && systemctl is-active --quiet "$UNIT" 2>/dev/null; then active=yes; fi
printf '\nOperation       : %s\nSource          : %s\nExecutable      : %s\nweb source      : %s\nMPEG-TS player  : %s\nDestination     : %s\nRunning service : %s\nRestart service : %s\napt dependencies: %s\n' \
    "$MODE" "$SOURCE_DIR" "$BINARY" "$WEB_DIR" "$MPEGTS_SOURCE" "$INSTALL_DIR" "$active" "$RESTART" "$INSTALL_DEPS"
[[ -z "$PLUGIN" ]] || printf 'CA plugin       : %s\n' "$PLUGIN"

if [[ "$MODE" == update && "$active" == yes && "$RESTART" == no ]]; then
    echo 'WARNING: Existing process will keep running, but web files become visible immediately.'
    echo 'Its old HTTP code may be incompatible with the new web assets until you restart it.'
    if (( ! DRY_RUN )); then
        ask_yes 'Deploy anyway, without restarting the active service?' || fail 'Cancelled.'
    fi
fi

if (( DRY_RUN )); then
    echo; echo 'DRY RUN: no packages, files, services or configurations were modified.'
    exit 0
fi
(( EUID == 0 )) || fail 'Run this command with sudo (or as root).'
command -v systemctl >/dev/null 2>&1 || fail 'systemctl is not available.'

DEPS=(ca-certificates libcurl4-openssl-dev libjsoncpp-dev libssl-dev libcrypt-dev
      libdvbcsa-dev libboost-thread-dev gstreamer1.0-tools
      gstreamer1.0-plugins-base gstreamer1.0-plugins-good
      gstreamer1.0-plugins-bad gstreamer1.0-plugins-ugly
      gstreamer1.0-libav gstreamer1.0-rtsp gstreamer1.0-vaapi
      vainfo intel-media-va-driver)
if [[ "$INSTALL_DEPS" == yes ]]; then
    command -v apt-get >/dev/null 2>&1 || fail 'apt-get is required for installing dependencies.'
    apt-get update
    env DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends "${DEPS[@]}"
fi
# Check ELF dependencies before deploying or restarting any existing service.
if command -v ldd >/dev/null 2>&1; then
    objects=("$BINARY")
    [[ -z "$PLUGIN" ]] || objects+=("$PLUGIN")
    for obj in "${objects[@]}"; do
        missing="$(ldd "$obj" 2>/dev/null | grep 'not found' || true)"
        [[ -z "$missing" ]] || fail "Missing shared libraries for $obj: $missing"
    done
fi

# Prepare full replacement before changing live files. Copy existing web first so
# vendor files omitted from a release ZIP survive. No channel configuration copied.
mkdir -p "$INSTALL_DIR"
STAGE="$(mktemp -d "$INSTALL_DIR/.preview-upgrade.XXXXXXXX")"
BACKUP_ROOT="$INSTALL_DIR/.installer-backups"
BACKUP="$BACKUP_ROOT/$(date +%Y%m%d-%H%M%S)-$$"
mkdir -p "$BACKUP"

BIN_CHANGED=0
WEB_CHANGED=0
PLUGIN_CHANGED=0
OSCAM_CHANGED=0
DEPLOY_SUCCESS=0
WAS_ACTIVE="$active"
cleanup() { [[ -z "${STAGE:-}" || ! -e "$STAGE" ]] || rm -rf -- "$STAGE"; }
rollback() {
    local rc="$1"
    trap - ERR INT TERM
    echo "ERROR: deployment failed (exit $rc). Attempting to restore overwritten program files." >&2
    if (( WEB_CHANGED )); then
        if [[ -d "$BACKUP/web.previous" ]]; then
            rm -rf -- "$INSTALL_DIR/web"
            mv -- "$BACKUP/web.previous" "$INSTALL_DIR/web" || true
        elif [[ -d "$BACKUP/web" ]]; then
            rm -rf -- "$INSTALL_DIR/web"
            cp -a -- "$BACKUP/web" "$INSTALL_DIR/web" || true
        else
            rm -rf -- "$INSTALL_DIR/web"
        fi
    fi
    if (( BIN_CHANGED )); then
        if [[ -f "$BACKUP/$APP" ]]; then
            cp -a -- "$BACKUP/$APP" "$STAGE/$APP.restore" && mv -f -- "$STAGE/$APP.restore" "$INSTALL_DIR/$APP" || true
        else rm -f -- "$INSTALL_DIR/$APP"; fi
    fi
    if (( PLUGIN_CHANGED )); then
        if [[ -f "$BACKUP/tvstreammersat5-ca-newcamd.so" ]]; then
            cp -a -- "$BACKUP/tvstreammersat5-ca-newcamd.so" "$INSTALL_DIR/ca-plugins/tvstreammersat5-ca-newcamd.so" || true
        else rm -f -- "$INSTALL_DIR/ca-plugins/tvstreammersat5-ca-newcamd.so"; fi
    fi
    if (( OSCAM_CHANGED )); then
        if [[ -f "$BACKUP/oscam-mini" ]]; then
            cp -a -- "$BACKUP/oscam-mini" "$INSTALL_DIR/oscam-mini/oscam-mini" || true
        else rm -f -- "$INSTALL_DIR/oscam-mini/oscam-mini"; fi
    fi
    if [[ "$WAS_ACTIVE" == yes && "$RESTART" == yes ]]; then
        systemctl restart "$UNIT" || echo "WARNING: original service did not restart; inspect journalctl -u $UNIT" >&2
    fi
    cleanup
    exit "$rc"
}
trap 'rollback $?' ERR
trap 'rollback 130' INT
trap 'rollback 143' TERM
trap cleanup EXIT

install -m 0755 "$BINARY" "$STAGE/$APP"
mkdir -p "$STAGE/web"
if [[ -d "$INSTALL_DIR/web" ]]; then cp -a -- "$INSTALL_DIR/web/." "$STAGE/web/"; fi
cp -a -- "$WEB_DIR/." "$STAGE/web/"
[[ -s "$STAGE/web/vendor/mpegts.min.js" && -s "$STAGE/web/preview/preview-player.js" ]] || \
    fail 'Staged web content is incomplete.'
if [[ -n "$PLUGIN" ]]; then
    install -m 0644 "$PLUGIN" "$STAGE/tvstreammersat5-ca-newcamd.so"
fi
if [[ -n "$OSCAM_BINARY" ]]; then
    install -m 0755 "$OSCAM_BINARY" "$STAGE/oscam-mini"
fi

# Do not touch production until every asset has been staged and verified.
if [[ -f "$INSTALL_DIR/$APP" ]]; then cp -a -- "$INSTALL_DIR/$APP" "$BACKUP/$APP"; fi
if [[ -d "$INSTALL_DIR/web" ]]; then
    cp -a -- "$INSTALL_DIR/web" "$BACKUP/web"   # persistent rollback copy
fi
if [[ -n "$PLUGIN" && -f "$INSTALL_DIR/ca-plugins/tvstreammersat5-ca-newcamd.so" ]]; then
    cp -a -- "$INSTALL_DIR/ca-plugins/tvstreammersat5-ca-newcamd.so" "$BACKUP/tvstreammersat5-ca-newcamd.so"
fi
if [[ -n "$OSCAM_BINARY" && -f "$INSTALL_DIR/oscam-mini/oscam-mini" ]]; then
    cp -a -- "$INSTALL_DIR/oscam-mini/oscam-mini" "$BACKUP/oscam-mini"
fi

log 'Deploying executable, complete web/ tree and optional binaries'
BIN_CHANGED=1
mv -f -- "$STAGE/$APP" "$INSTALL_DIR/$APP"
WEB_CHANGED=1
if [[ -d "$INSTALL_DIR/web" ]]; then
    mv -- "$INSTALL_DIR/web" "$BACKUP/web.previous"  # fast, same filesystem
fi
mv -- "$STAGE/web" "$INSTALL_DIR/web"
if [[ -n "$PLUGIN" ]]; then
    install -d -m 0755 "$INSTALL_DIR/ca-plugins"
    PLUGIN_CHANGED=1
    mv -f -- "$STAGE/tvstreammersat5-ca-newcamd.so" "$INSTALL_DIR/ca-plugins/tvstreammersat5-ca-newcamd.so"
fi
if [[ -n "$OSCAM_BINARY" ]]; then
    install -d -m 0755 "$INSTALL_DIR/oscam-mini/config"
    OSCAM_CHANGED=1
    mv -f -- "$STAGE/oscam-mini" "$INSTALL_DIR/oscam-mini/oscam-mini"
fi

if [[ "$MODE" == install ]]; then
    # Preserve any pre-existing customized service file.
    if ! systemctl list-unit-files "$UNIT" --no-legend 2>/dev/null | grep -q -F "$UNIT"; then
        install -d -m 0755 /etc/systemd/system
        cat > "/etc/systemd/system/$UNIT" <<EOFUNIT
[Unit]
Description=TVStreammerSAT5 streaming service
Wants=network-online.target
After=network-online.target

[Service]
Type=simple
User=root
Group=root
WorkingDirectory=$INSTALL_DIR
ExecStart=$INSTALL_DIR/$APP
Restart=on-failure
RestartSec=3
TimeoutStopSec=35
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
EOFUNIT
    else
        echo "Existing $UNIT unit preserved (check ExecStart and WorkingDirectory)."
    fi
fi

if [[ "$RESTART" == yes ]]; then
    log 'Restarting TVStreammerSAT5 (this interrupts the channels)'
    systemctl daemon-reload
    systemctl enable "$UNIT" >/dev/null
    systemctl restart "$UNIT"
    sleep 2
    systemctl is-active --quiet "$UNIT" || fail 'Service did not become active.'
else
    echo 'Service left untouched. Restart manually in a maintenance window.'
fi
DEPLOY_SUCCESS=1
trap - ERR INT TERM
cleanup
printf '\nSUCCESS: %s deployed to %s\n' "$MODE" "$INSTALL_DIR"
printf 'web assets: %s/web (including preview/, vendor/, mpegts.min.js)\n' "$INSTALL_DIR"
printf 'Rollback backup: %s\n' "$BACKUP"
printf 'Service status: systemctl status %s --no-pager\n' "$UNIT"
if [[ "$RESTART" == no ]]; then
    printf 'Later, when safe: sudo systemctl restart %s\n' "$UNIT"
fi
