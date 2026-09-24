#!/usr/bin/env bash
set -euo pipefail

SRC="${1:?vendored source directory is required}"
OUT="${2:?output directory is required}"
WORK="$OUT/src"
BUILD="$OUT/cmake-build"
BIN_OUT="$OUT/oscam-mini"

for cmd in cmake gcc make pkg-config; do
  command -v "$cmd" >/dev/null || { echo "Missing build dependency: $cmd" >&2; exit 1; }
done

if [[ ! -f "$SRC/config.sh" || ! -f "$SRC/CMakeLists.txt" ]]; then
  echo "ERROR: OSCam source is incomplete in: $SRC" >&2
  echo "Expected at least config.sh and CMakeLists.txt." >&2
  echo "Commit the complete third_party/oscam-mini tree to the repository." >&2
  exit 2
fi

# PC/SC smart-card reader support is mandatory for this build; fail loudly rather
# than distributing a binary without OMNIKEY support.
if ! pkg-config --exists libpcsclite || [[ ! -f /usr/include/PCSC/wintypes.h ]]; then
  echo "OSCam-mini PC/SC requires libpcsclite-dev (and pcscd at runtime)." >&2
  echo "Install: sudo apt-get install libpcsclite-dev pcscd pcsc-tools" >&2
  exit 4
fi

rm -rf "$WORK" "$BUILD"
mkdir -p "$OUT"
cp -a "$SRC" "$WORK"
mkdir -p "$WORK/Distribution" "$WORK/webif"
chmod +x "$WORK/config.sh"

cd "$WORK"
./config.sh --disable all
# Enable the card-system handlers shipped by this OSCam snapshot.  Only Newcamd
# remains a network listener; enabling readers does not grant new access rights.
./config.sh --enable MODULE_NEWCAMD readers CARDREADER_PHOENIX

printf '\nEnabled OSCam-mini modules:\n'
./config.sh --show-enabled all

# Build via OSCam's CMakeLists instead of relying on the upstream root Makefile.
# This also avoids failures when an archive was unpacked through Windows and file
# permissions or the Makefile were lost.
cmake -S "$WORK" -B "$BUILD" \
  -DCMAKE_BUILD_TYPE=Release \
  -DHAVE_PCSC=1 \
  -DCS_CONFDIR=/opt/TVStreammerSAT5/oscam-mini/config
# Detect any unexpected configure fallback to non-PC/SC mode.
if ! grep -Eq '(^CONFIG_CARDREADER_PCSC=y$|^USE_PCSC[=: ]|^WITH_PCSC[=: ]|^HAVE_PCSC(:INTERNAL|:UNINITIALIZED|:BOOL)?=1$)' \
     "$WORK/config.mak" "$BUILD/config.mak" "$BUILD/CMakeCache.txt" 2>/dev/null; then
  echo "OSCam-mini PC/SC support not confirmed after CMake configuration" >&2
  exit 5
fi
cmake --build "$BUILD" --target oscam -j"$(nproc)"

if [[ ! -x "$BUILD/oscam" ]]; then
  echo "ERROR: OSCam binary was not produced at $BUILD/oscam" >&2
  exit 3
fi

install -m0755 "$BUILD/oscam" "$BIN_OUT"
echo "OSCam-mini built: $BIN_OUT"
