# TVStreammerSAT5 203.73: private satellite preview

Target: selected DVB service already presented as a single-program MPEG-TS in
TVStreammerSAT5 203.72, where production output is healthy but the private HTTP
preview returns HTTP 200 with no browser media buffer.

Only the **synthetic on-demand** localhost HTTP preview of a selected DVB SPTS
bypasses the generic HTTP `tsdemux -> mpegtsmux` branch. It relays the TS from
this channel's existing source tee through bounded leaky queues and a private
`tcpserversink` instead. It does not reselect SID, reconstruct the program,
change the source, decrypt cards, alter public output configuration, or launch
new transcoders. Output branching is built when a channel pipeline starts, so
an already-running pipeline needs its own controlled restart to use this path.

For a newly started stream, `SAT PREVIEW 203.73 ... result=ready` means only
that the GStreamer branch linked successfully. After opening preview, look for
`HTTP PREVIEW DELIVERY 203.73 ... first-upstream-bytes` to confirm the relay
actually delivered data; or `no-upstream-media-after-10s` to confirm the
private relay did not receive any bytes. `relay-finished` logs total bytes for
that HTTP session only. These logs contain neither authorization nor session
identifiers.

This change does **not** convert MPEG-2/AC3 (or other browser-incompatible
codecs) to H.264/AAC. Browser rendering is not proven by HTTP 200 or by a
first-upstream-bytes log. A separate, on-demand browser-codec transcoder may
still be necessary if TS bytes flow but videoWidth remains zero.

Build/test limitations: the source-only JS regression does not exercise
GStreamer or physical DVB hardware. Verify the full app build and test against
one isolated canary channel before any deployment.
