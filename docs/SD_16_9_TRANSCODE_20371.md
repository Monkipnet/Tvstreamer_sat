# SD 16:9 video transcoding — 203.71

This change adds two **separate** resolution presets while retaining the existing
`720x576` preset unchanged for compatibility with saved channel settings:

| `transcode_resolution` | Encoded raster | Raw video pixel-aspect-ratio | Display aspect ratio |
|---|---|---|---|
| `1024x576` | 1024×576 | 1:1 | 16:9 |
| `720x576_16_9` | 720×576 | 64:45 | 16:9 |
| `720x576` (existing) | 720×576 | 1:1 (existing behavior) | 5:4 |

`1024x576` is the least ambiguous choice for clients that ignore H.264 aspect
ratio signaling. `720x576_16_9` uses anamorphic PAL SD and requires the selected
encoder to propagate the pixel aspect ratio into H.264 VUI and downstream
players to honor it. Check a short captured encoded output with `ffprobe`
for SAR=64:45 and DAR=16:9 before production use of the anamorphic preset,
especially with Intel/NVIDIA hardware encoders. Some encoders may not preserve SAR.

The selection is available in the web UI under video transcode resolution. Both
transcode implementations (in-process bin and shared gst-launch process) use the
same preset-to-geometry mapping, and recommended video bitrates are 2500 kbps
(square-pixel) and 2000 kbps (anamorphic). The user's configured bitrate is not
changed unless the UI resolution selector invokes the existing bitrate-default
handler.

The source has not been integrated with real OSCAM PC/SC or preview changes.
Build and test the new preset independently before replacing a running binary.
