# TVStreammerSAT5 203.68 — forced temporary HTTP preview disconnect

This patch is based on 203.67 plus the `idle-preview-gate` update, not on a
clean pre-preview tree. Apply it once to that exact baseline.

## Why idle preview could stay active for ~1 minute

The relay thread previously blocked in `read(upstreamFd)`. After the browser
closed its streaming GET, no new upstream TS packet was required to arrive, so
server-side `privatePreviewDemand` could remain nonzero. Closing the modal
alone could not wake the read. The extra HTTP branch then remained ungated.

## What changes

* Each **private** `/api/streams/<id>/preview.ts` GET has a per-viewer 128-bit
  random session token (`?session=<32-hex>`). The public configured HTTP output
  has no private preview token and is not affected.
* On modal close, Escape, tile change, or `pagehide`, the browser destroys its
  mpegts.js player and sends an authenticated, short `keepalive` POST to
  `/api/streams/<id>/preview/close` with JSON `{"session":"..."}`.
* The backend matches **exactly one** stream/token pair and calls `shutdown()`
  on both the browser and internal relay sockets. The relay thread is the sole
  owner of `close()`, so worker cleanup decrements demand without duplicate
  decrement or disconnecting another viewer.
* A 90-second, capped cancellation marker prevents an already-requested GET
  from reactivating the preview if POST arrives before the upstream connect.
* Independently of POST, the relay checks for browser disconnect at most every
  250ms even if the upstream TS socket is idle. A 1-second send timeout limits
  stalls on an unresponsive preview viewer. Normal configured HTTP relay
  clients are not subject to this preview-only polling/timeout.
* `/api/state` reports `203.68` via `src/AppVersion.h`; the About panel includes
  a clickable contact email (currently `monkipnet@gmail.com`, carried over
  from the existing source; replace it only after the owner confirms another).

## Important limitation

This patch **does not dynamically destroy the optional GStreamer output
branch**. The in-process preview pad probe drops its buffers when no viewers
remain, but the allocated branch elements stay until the stream itself stops.
For separately transcoded channels, the auxiliary output in the gst-launch
child also remains allocated. Do not present this patch as full on-demand
GStreamer branch creation/deletion or guaranteed reclamation of all idle RAM
or CPU. Achieving that for child transcoders requires a separate design that
must not restart or disturb any primary 43-channel output.

## Validation before deploying to production

1. Build in a *new build directory*, without replacing the current binary.
2. Test one non-transcoded and one separately transcoded channel on a
   **separate, authorized test instance**. Open/close rapidly 10 times.
3. Watch journal for `HTTP PREVIEW CLOSE 203.68` and examine active HTTP
   session count. Test with two browser windows: closing one must not stop the
   other. Ensure public configured HTTP, SRT, HLS and UDP outputs remain
   unchanged.
4. Compare CPU, RSS, and child-process RSS across a longer interval with all
   preview windows closed. Pipeline-branch allocations may remain.
5. Only after testing decide on a controlled replacement of the live binary.

Do not send arbitrary close tokens or stop the service to test this change.
