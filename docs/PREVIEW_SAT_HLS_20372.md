# Browser preview 203.72: HLS input and satellite channels

- The authenticated preview API offers an existing HLS playlist when a channel
  has a configured HLS output. For incoming HLS, the browser prefers that
  existing HLS output (native HLS or local hls.js), avoiding an additional
  MPEG-TS remux path. Other channels retain the existing private MPEG-TS relay. If its player
  reports an error and the channel has an existing configured HLS output, the
  browser tries that output without restarting or duplicating the live source.
- Install **both** local preview libraries using `bash scripts/vendor_preview_libs.sh`
  in the source tree during preparation, and ensure installed web resources
  are available to the running service. The archive intentionally does not
  vendor third-party JS bundles without their verified upstream files.
- The browser cannot generally decode satellite MPEG-2 video, AC3 audio or
  scrambled MPEG-TS directly via mpegts.js. This change does **not** add an
  on-demand H.264/AAC preview transcoder, and does not promise preview of
  those channels. Check source codec and entitlement before attributing a
  black preview tile to a relay defect.
- HLS fallback requires an **existing HLS output**; it is not a new private
  HLS source and does not enable playback when no playable media reaches the
  configured playlist. The configured HLS URL still obeys existing subscriber
  access restrictions. No source credentials are copied into the manifest.
- No running stream, global service or card-reader process is restarted by
  opening or closing the browser preview.

For full satellite preview of MPEG-2/AC3/HEVC, a separately designed and
measured per-viewer on-demand transcoder is still required. Do not enable a
second transcoder on every production channel.
