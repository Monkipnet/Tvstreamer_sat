# Offline browser-player libraries

Run `bash scripts/vendor_preview_libs.sh` **once on a development/build machine with Internet access** to download pinned hls.js and mpegts.js UMD builds and their Apache-2.0 license texts here. Commit these generated files together with the TVStreammer code. The TVStreammer server serves them from its own `/preview/` paths; the browser needs no CDN and no external media player.

For a built binary in `build/TVStreammerSAT5`, these files must be under `web/vendor/` in the project root. For a deployed `/opt/TVStreammerSAT5/TVStreammerSAT5` binary, place them at `/opt/TVStreammerSAT5/web/vendor/`.

The project archive does not contain upstream hls.js/mpegts.js builds because the package was assembled without access to download their distributable files; use the vendor script before deploying browser preview. Safari native-HLS may play `.m3u8` without hls.js.
