# TVStreammerSAT5 v203.67 — temporary HTTP MPEG-TS browser preview

Double-click a running stream tile to open one browser preview. The dialog has no SRT/HLS/UDP selector: it always plays MPEG-TS over HTTP with the bundled `mpegts.js` library and the browser MediaSource API.

If the channel already has an HTTP MPEG-TS output, the preview reuses `/stream/<id>.ts`. If the channel has no configured HTTP output, the running media pipeline contains a private localhost-only HTTP relay for preview. The authenticated control-panel endpoint is `/api/streams/<id>/preview.ts`; it is not advertised as a production output and is not written back into the stream configuration.

The private preview branch is generated as part of the channel's normal pipeline construction. Opening or closing the browser dialog does not restart the channel, create a second input, decoder, video encoder, or audio encoder. The preview branch uses a short leaky queue so a slow browser cannot back-pressure the production SRT/HLS/UDP branches. When there is no browser client, `tcpserversink` has no external consumer and the control-panel HTTP relay sends no preview bytes to a remote client.

The existing configured SRT/HLS/UDP outputs and their settings remain unchanged. The private `/api/streams/...` endpoint always requires the control-panel authentication rules. A stopped stream does not get a preview source.

The double click ignores buttons, links, form controls and content-editable elements. Closing the dialog destroys the `mpegts.js` MediaSource player and closes its HTTP request.

`web/preview/preview-player.js` and `.css` are editing/test copies; their contents are embedded in `src/HttpServer.cpp` and must remain identical. `web/vendor/mpegts.min.js` is served locally by TVStreammerSAT5; no CDN is required while viewing.

## Test and build

```bash
cd /home/svettv/Tvstreamer_sat
node tests/test_browser_preview.js
wc -c web/vendor/mpegts.min.js
cmake -S . -B build-preview-temp-http -DCMAKE_BUILD_TYPE=Release -DTVSTREAMMERSAT5_BUILD_OSCAM_MINI=OFF
nice -n 10 cmake --build build-preview-temp-http --parallel 2 --target TVStreammerSAT5 tvstreammersat5_ca_newcamd
```

Build output is separate from the installed service. Building does not replace the running binary. Deployment/restart remains a separate maintenance action.
