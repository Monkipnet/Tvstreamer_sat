# Local browser-preview library

The HTTP-only browser preview uses `mpegts.min.js` (mpegts.js, UMD build) and the browser's MediaSource support. Keep the built JS file at `web/vendor/mpegts.min.js` in the project and deploy it under `web/vendor/` alongside the executable. The browser receives it from `/preview/mpegts.min.js`; no external player or browser-side CDN is used.

If the library is not already present, `bash scripts/vendor_preview_libs.sh` fetches the pinned library and its license on an Internet-connected build machine. The script may also fetch hls.js for compatibility with older builds; this HTTP-only preview does not load hls.js.

Existing checked-in `web/vendor/*.js` files on a Git working copy must NOT be deleted or overwritten by unpacking an archive that omits them. Apply the supplied source patch instead.
