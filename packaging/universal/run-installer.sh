#!/usr/bin/env bash
set -Eeuo pipefail

# Runtime half of the self-extracting .run package. It is executed from an
# extracted payload directory created by build_universal_run.sh.

ORIG_ARGS=("$@")
PAYLOAD_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INSTALL_DIR="${TVS_INSTALL_DIR:-/opt/TVStreammerSAT5}"
STATE_DIR="${TVS_INSTALLER_STATE_DIR:-/var/lib/tvstreammersat5-installer}"
NO_START=0
OFFLINE=0
DRY_RUN=0
NO_PCSC=0

usage() {
  cat <<'USAGE'
TVStreammerSAT5 universal .run installer

Usage: sudo ./TVStreammerSAT5-*.run [options]

Options:
  --install-dir DIR  Install prefix (default /opt/TVStreammerSAT5)
  --no-start         Install files but do not start/restart services
  --offline          Do not use apt/dnf/yum/zypper/pacman
  --no-pcsc          Do not install/check PC/SC daemon and CCID reader support
  --dry-run          Print actions without changing the system
  -h, --help         Show this help

Supported targets: glibc-based x86_64/aarch64 Linux. Debian/Ubuntu, Fedora/RHEL/
Rocky/Alma, openSUSE/SLES and Arch package managers are detected for host-only
PC/SC/CCID dependencies. Bundled application/GStreamer libraries are used for
all application runtime dependencies. Alpine/musl is intentionally rejected.
USAGE
}

while (($#)); do
  case "$1" in
    --install-dir) INSTALL_DIR="$2"; shift 2 ;;
    --no-start) NO_START=1; shift ;;
    --offline) OFFLINE=1; shift ;;
    --no-pcsc) NO_PCSC=1; shift ;;
    --dry-run) DRY_RUN=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
done

if [[ $EUID -ne 0 ]]; then
  command -v sudo >/dev/null 2>&1 || { echo "Run as root (sudo is not installed)." >&2; exit 1; }
  exec sudo -E bash "$0" "${ORIG_ARGS[@]}"
fi

run() {
  if (( DRY_RUN )); then printf '+ '; printf '%q ' "$@"; printf '\n'; else "$@"; fi
}

[[ -r /etc/os-release ]] && . /etc/os-release || true
ARCH_EXPECTED="$(cat "$PAYLOAD_DIR/ARCH")"
ARCH_ACTUAL="$(uname -m)"
[[ "$ARCH_EXPECTED" == "$ARCH_ACTUAL" ]] || {
  echo "Architecture mismatch: package=$ARCH_EXPECTED host=$ARCH_ACTUAL" >&2; exit 1;
}

if ldd --version 2>&1 | head -1 | grep -qi musl || [[ -e /lib/ld-musl-${ARCH_ACTUAL}.so.1 ]]; then
  echo "This .run is for glibc Linux. Alpine/musl needs a separately built package." >&2
  exit 1
fi

# Verify that the self-extracted payload was not corrupted.
if ! (cd "$PAYLOAD_DIR" && sha256sum -c SHA256SUMS >/dev/null); then
  echo "Payload checksum verification failed." >&2
  exit 1
fi

VERSION="$(cat "$PAYLOAD_DIR/VERSION")"
echo "TVStreammerSAT5 $VERSION universal installer"
echo "Host: ${PRETTY_NAME:-Linux} / $ARCH_ACTUAL"
echo "Install dir: $INSTALL_DIR"

# Only host-coupled facilities are installed from the target distribution:
# pcscd and the CCID USB driver. Application, OpenSSL, Boost, curl, jsoncpp,
# dvbcsa, GStreamer core/plugins/codecs and libpcsclite userspace client are in
# the payload. GPU/DVB kernel drivers are intentionally host-provided.
install_pcsc_packages() {
  (( NO_PCSC )) && return 0

  if command -v pcscd >/dev/null 2>&1 && \
     { [[ -d /usr/lib/pcsc/drivers ]] || [[ -d /usr/lib64/pcsc/drivers ]] || find /usr/lib /usr/lib64 -maxdepth 4 -type d -path "*/pcsc/drivers" -print -quit 2>/dev/null | grep -q .; }; then
    echo "PC/SC daemon and reader driver directory already present."
    return 0
  fi

  if (( OFFLINE )); then
    echo "Offline mode: PC/SC host packages are missing. Install pcscd + CCID driver manually." >&2
    return 1
  fi

  if command -v apt-get >/dev/null 2>&1; then
    run apt-get update
    run env DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends pcscd libccid ca-certificates
    # pcsc-tools is diagnostic only; do not make installation fail if a minimal
    # distribution does not publish it.
    run env DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends pcsc-tools || true
  elif command -v dnf >/dev/null 2>&1; then
    run dnf -y install pcsc-lite pcsc-lite-ccid ca-certificates
    run dnf -y install pcsc-tools || true
  elif command -v yum >/dev/null 2>&1; then
    run yum -y install pcsc-lite pcsc-lite-ccid ca-certificates
    run yum -y install pcsc-tools || true
  elif command -v zypper >/dev/null 2>&1; then
    run zypper --non-interactive install pcsc-lite pcsc-ccid ca-certificates
    run zypper --non-interactive install pcsc-tools || true
  elif command -v pacman >/dev/null 2>&1; then
    run pacman -Syu --needed --noconfirm pcsclite ccid ca-certificates
    run pacman -S --needed --noconfirm pcsc-tools || true
  else
    echo "Unknown package manager. Install PC/SC daemon + CCID driver manually, or rerun with --no-pcsc." >&2
    return 1
  fi
}

install_pcsc_packages

# Save only files the installer replaces. User configuration is never included
# in the backup copy list because it is not overwritten.
STAMP="$(date +%Y%m%d-%H%M%S)"
BACKUP="$STATE_DIR/backups/$STAMP"
run mkdir -p "$BACKUP" "$STATE_DIR"
for f in \
  "$INSTALL_DIR/TVStreammerSAT5" \
  "$INSTALL_DIR/tvstreammersat5-run" \
  "$INSTALL_DIR/oscam-mini/oscam-mini" \
  "$INSTALL_DIR/oscam-mini/oscam-mini-run" \
  /etc/systemd/system/tvstreammersat5.service \
  /etc/systemd/system/oscam-mini.service; do
  if [[ -e "$f" ]]; then
    rel="${f#/}"
    run mkdir -p "$BACKUP/$(dirname "$rel")"
    run cp -a "$f" "$BACKUP/$rel"
  fi
done

# Stop only the services being replaced. Do not touch unrelated channels or
# processes. --no-start can be used when the operator wants a manual cutover.
if command -v systemctl >/dev/null 2>&1; then
  run systemctl stop tvstreammersat5.service || true
  run systemctl stop oscam-mini.service || true
elif command -v rc-service >/dev/null 2>&1; then
  run rc-service tvstreammersat5 stop || true
  run rc-service oscam-mini stop || true
elif command -v service >/dev/null 2>&1; then
  run service tvstreammersat5 stop || true
  run service oscam-mini stop || true
fi

run mkdir -p "$INSTALL_DIR" "$INSTALL_DIR/ca-plugins" "$INSTALL_DIR/oscam-mini/config" \
             "$INSTALL_DIR/oscam-mini/default-config"
run install -m0755 "$PAYLOAD_DIR/app/TVStreammerSAT5" "$INSTALL_DIR/TVStreammerSAT5"
run install -m0755 "$PAYLOAD_DIR/app/tvstreammersat5-run" "$INSTALL_DIR/tvstreammersat5-run"
run install -m0755 "$PAYLOAD_DIR/app/ca-plugins/tvstreammersat5-ca-newcamd.so" "$INSTALL_DIR/ca-plugins/tvstreammersat5-ca-newcamd.so"
run install -m0755 "$PAYLOAD_DIR/app/oscam-mini/oscam-mini" "$INSTALL_DIR/oscam-mini/oscam-mini"
run install -m0755 "$PAYLOAD_DIR/app/oscam-mini/oscam-mini-run" "$INSTALL_DIR/oscam-mini/oscam-mini-run"

# Atomic-ish runtime replacement: copy into a temporary directory, then rename.
RUNTIME_TMP="$INSTALL_DIR/.runtime.new.$STAMP"
run rm -rf "$RUNTIME_TMP"
run cp -a "$PAYLOAD_DIR/app/runtime" "$RUNTIME_TMP"
if (( ! DRY_RUN )); then
  rm -rf "$INSTALL_DIR/runtime.old"
  [[ -d "$INSTALL_DIR/runtime" ]] && mv "$INSTALL_DIR/runtime" "$INSTALL_DIR/runtime.old"
  mv "$RUNTIME_TMP" "$INSTALL_DIR/runtime"
else
  echo "+ mv '$RUNTIME_TMP' '$INSTALL_DIR/runtime'"
fi

if (( ! DRY_RUN )); then
  cp -a "$PAYLOAD_DIR/app/oscam-mini/default-config/." "$INSTALL_DIR/oscam-mini/default-config/"
  for cfg in oscam.conf oscam.server oscam.user; do
    [[ -e "$INSTALL_DIR/oscam-mini/config/$cfg" ]] || \
      cp -a "$PAYLOAD_DIR/app/oscam-mini/default-config/$cfg" "$INSTALL_DIR/oscam-mini/config/$cfg"
  done
else
  echo "+ seed OSCam-mini config only when missing"
fi

# Host UDP tuning is safe to install on all supported distributions; applying it
# is best-effort because containers/minimal systems may not expose every sysctl.
run install -m0644 "$PAYLOAD_DIR/packaging/99-tvstreammer-udp.conf" /etc/sysctl.d/99-tvstreammer-udp.conf
run sysctl --system >/dev/null 2>&1 || true

install_systemd() {
  run mkdir -p /etc/systemd/system
  if (( ! DRY_RUN )); then
    cat > /etc/systemd/system/tvstreammersat5.service <<UNIT
[Unit]
Description=TVStreammerSAT5 IPTV/DVB streaming server
Wants=network-online.target
After=network-online.target pcscd.service

[Service]
Type=simple
User=root
Group=root
WorkingDirectory=$INSTALL_DIR
ExecStart=$INSTALL_DIR/tvstreammersat5-run
Restart=on-failure
RestartSec=3
TimeoutStopSec=35
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
UNIT
    cat > /etc/systemd/system/oscam-mini.service <<UNIT
[Unit]
Description=TVStreammerSAT5 OSCam-mini Newcamd/PCSC/Phoenix card server
After=network.target pcscd.service
Conflicts=oscam.service
ConditionPathExists=$INSTALL_DIR/oscam-mini/config/oscam.conf

[Service]
Type=simple
User=root
Group=root
WorkingDirectory=$INSTALL_DIR/oscam-mini
ExecStart=$INSTALL_DIR/oscam-mini/oscam-mini-run -c $INSTALL_DIR/oscam-mini/config
SuccessExitStatus=15 SIGTERM
Restart=on-failure
RestartSec=2
Nice=5
LimitNOFILE=1024

[Install]
WantedBy=multi-user.target
UNIT
  else
    echo "+ write systemd units"
  fi
  run systemctl daemon-reload
  run systemctl enable tvstreammersat5.service oscam-mini.service
  if systemctl list-unit-files oscam.service >/dev/null 2>&1; then run systemctl disable --now oscam.service || true; fi
  if (( ! NO_START )); then
    run systemctl restart oscam-mini.service
    run systemctl restart tvstreammersat5.service
  fi
}

install_openrc() {
  for name in tvstreammersat5 oscam-mini; do
    if (( ! DRY_RUN )); then
      cat > "/etc/init.d/$name" <<RC
#!/sbin/openrc-run
command="$INSTALL_DIR/$([[ $name == tvstreammersat5 ]] && echo tvstreammersat5-run || echo oscam-mini/oscam-mini-run)"
$([[ $name == oscam-mini ]] && echo 'command_args="-c '$INSTALL_DIR'/oscam-mini/config"' || true)
command_background="no"
pidfile="/run/$name.pid"
depend() { need net; }
RC
      chmod 0755 "/etc/init.d/$name"
    else
      echo "+ write /etc/init.d/$name"
    fi
    run rc-update add "$name" default || true
  done
  if (( ! NO_START )); then run rc-service oscam-mini restart; run rc-service tvstreammersat5 restart; fi
}

install_sysv() {
  # Minimal LSB scripts for legacy glibc systems without systemd/OpenRC.
  for name in tvstreammersat5 oscam-mini; do
    bin="$INSTALL_DIR/$([[ $name == tvstreammersat5 ]] && echo tvstreammersat5-run || echo oscam-mini/oscam-mini-run)"
    args=""; [[ $name == oscam-mini ]] && args="-c $INSTALL_DIR/oscam-mini/config"
    if (( ! DRY_RUN )); then
      cat > "/etc/init.d/$name" <<SYSV
#!/bin/sh
### BEGIN INIT INFO
# Provides:          $name
# Required-Start:    \$network
# Required-Stop:     \$network
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
### END INIT INFO
case "\$1" in
  start) start-stop-daemon --start --background --make-pidfile --pidfile /run/$name.pid --exec "$bin" -- $args ;;
  stop)  start-stop-daemon --stop --retry TERM/20/KILL/5 --pidfile /run/$name.pid ;;
  restart) "\$0" stop; sleep 1; "\$0" start ;;
  status) test -s /run/$name.pid && kill -0 "\$(cat /run/$name.pid)" ;;
  *) echo "Usage: \$0 {start|stop|restart|status}"; exit 2 ;;
esac
SYSV
      chmod 0755 "/etc/init.d/$name"
    else
      echo "+ write SysV init script $name"
    fi
  done
  command -v update-rc.d >/dev/null 2>&1 && { run update-rc.d tvstreammersat5 defaults; run update-rc.d oscam-mini defaults; }
  command -v chkconfig >/dev/null 2>&1 && { run chkconfig --add tvstreammersat5; run chkconfig --add oscam-mini; }
  if (( ! NO_START )); then run service oscam-mini restart; run service tvstreammersat5 restart; fi
}

if command -v systemctl >/dev/null 2>&1 && [[ -d /run/systemd/system ]]; then
  install_systemd
elif command -v rc-service >/dev/null 2>&1; then
  install_openrc
elif command -v service >/dev/null 2>&1 && command -v start-stop-daemon >/dev/null 2>&1; then
  install_sysv
else
  echo "No supported service manager found. Files were installed, but services were not registered." >&2
fi

# PC/SC can exist without pcsc_scan; verify daemon socket rather than requiring
# diagnostics. This supports minimal server distributions.
if (( ! NO_PCSC )); then
  if command -v systemctl >/dev/null 2>&1 && [[ -d /run/systemd/system ]]; then
    run systemctl enable pcscd.socket 2>/dev/null || true
    run systemctl start pcscd.socket 2>/dev/null || run systemctl start pcscd.service 2>/dev/null || true
  fi
  [[ -S /run/pcscd/pcscd.comm ]] || echo "Warning: PC/SC socket is not active yet; check pcscd before using OMNIKEY." >&2
fi

# Validate the packaged runtime in exactly the environment used by the service.
if (( ! DRY_RUN )); then
  export LD_LIBRARY_PATH="$INSTALL_DIR/runtime/lib"
  export GST_PLUGIN_SYSTEM_PATH_1_0=""
  export GST_PLUGIN_PATH_1_0="$INSTALL_DIR/runtime/gstreamer-1.0"
  export GST_PLUGIN_SCANNER="$INSTALL_DIR/runtime/libexec/gstreamer-1.0/gst-plugin-scanner"
  export GST_REGISTRY="$INSTALL_DIR/runtime/gstreamer-registry.bin"

  if ldd "$INSTALL_DIR/TVStreammerSAT5" | grep -q 'not found'; then
    echo "Missing application library:" >&2; ldd "$INSTALL_DIR/TVStreammerSAT5" | grep 'not found' >&2; exit 1
  fi
  if ldd "$INSTALL_DIR/oscam-mini/oscam-mini" | grep -q 'not found'; then
    echo "Missing OSCam-mini library:" >&2; ldd "$INSTALL_DIR/oscam-mini/oscam-mini" | grep 'not found' >&2; exit 1
  fi

  REQUIRED_GST=(udpsrc udpsink tsparse tsdemux mpegtsmux souphttpsrc hlsdemux hlssink srtsrc srtsink rtspsrc rtmpsrc x264enc avdec_h264)
  MISSING=()
  GST_INSPECT="$INSTALL_DIR/runtime/bin/gst-inspect-1.0"
  if [[ -x "$GST_INSPECT" ]]; then
    for e in "${REQUIRED_GST[@]}"; do
      "$GST_INSPECT" "$e" >/dev/null 2>&1 || MISSING+=("$e")
    done
    ((${#MISSING[@]}==0)) || echo "Warning: bundled GStreamer is missing elements: ${MISSING[*]}" >&2
  else
    echo "Warning: bundled gst-inspect-1.0 is missing; GStreamer validation skipped." >&2
  fi
fi

if (( ! DRY_RUN )); then
  mkdir -p "$STATE_DIR"
  printf '%s\n' "$INSTALL_DIR" > "$STATE_DIR/install-dir.txt"
  printf '%s\n' "$VERSION" > "$STATE_DIR/version.txt"
fi

echo
echo "TVStreammerSAT5 $VERSION installation complete."
echo "Application : $INSTALL_DIR/TVStreammerSAT5"
echo "OSCam-mini  : $INSTALL_DIR/oscam-mini/oscam-mini"
echo "Backup      : $BACKUP"
if command -v systemctl >/dev/null 2>&1 && [[ -d /run/systemd/system ]]; then
  echo "Status      : systemctl status tvstreammersat5 oscam-mini --no-pager"
fi
