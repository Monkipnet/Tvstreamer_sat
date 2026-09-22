#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
mkdir -p web/vendor
# Fetch into temp files and only replace existing verified files after both downloads succeed.
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
curl -fLsS --retry 3 --connect-timeout 10 --max-time 120 \
  'https://cdn.jsdelivr.net/npm/hls.js@1.6.15/dist/hls.min.js' -o "$work/hls.min.js"
curl -fLsS --retry 3 --connect-timeout 10 --max-time 120 \
  'https://cdn.jsdelivr.net/npm/mpegts.js@1.7.3/dist/mpegts.js' -o "$work/mpegts.min.js"
curl -fLsS --retry 3 --connect-timeout 10 --max-time 120 \
  'https://cdn.jsdelivr.net/npm/hls.js@1.6.15/LICENSE' -o "$work/LICENSE.hls.js"
curl -fLsS --retry 3 --connect-timeout 10 --max-time 120 \
  'https://cdn.jsdelivr.net/npm/mpegts.js@1.7.3/LICENSE' -o "$work/LICENSE.mpegts.js"
for lib in hls.min.js mpegts.min.js; do
  size=$(wc -c < "$work/$lib")
  if (( size < 100000 || size > 5000000 )); then
    echo "Invalid $lib size: $size" >&2
    exit 1
  fi
  if command -v node >/dev/null 2>&1; then node --check "$work/$lib"; fi
done
install -m 644 "$work/hls.min.js" web/vendor/hls.min.js
install -m 644 "$work/mpegts.min.js" web/vendor/mpegts.min.js
install -m 644 "$work/LICENSE.hls.js" web/vendor/LICENSE.hls.js
install -m 644 "$work/LICENSE.mpegts.js" web/vendor/LICENSE.mpegts.js
sha256sum web/vendor/*.js
printf '\nLibraries vendored. Commit web/vendor/* with the app source.\n'
