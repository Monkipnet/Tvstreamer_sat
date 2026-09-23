# TVStreammerSAT5 v203.67 – idle preview resource guard (candidate)

A production channel is not restarted on preview open/close. For **in-process**
GStreamer channels without configured HTTP, an isolated tee pad feeding the
synthetic localhost HTTP preview is gated with a buffer probe. With zero
connected HTTP preview sessions the probe drops data **before** the extra
remux, queues, pacing and TCP sink; the production tee pads do not change.
The first preview client opens the gate before it connects to the private
sink. The last HTTP relay thread closes the gate again. Failed connections
and failed thread launches decrement the counter, and the counter's shared
lifetime survives late relay cleanup when a channel is stopped. Concurrent
preview clients share the same gate.

**Limits:** the preview's GStreamer elements and localhost listening socket
are still allocated while the channel is on air; this patch removes their
idle processing, not all of their baseline RSS. Channels transcoded in a
separate `gst-launch-1.0` process retain the previous preview branch and its
resource use: toggling it requires a separately implemented child control
channel or a genuinely dynamic branch. This version is **not** a full
on-demand-create/destroy solution; do not claim complete memory recovery.

**Validation:** `node tests/test_browser_preview.js` is a JS and source
contract test only, **not** an integration test of the live 43-channel
GStreamer topology. Compile and run on a staging instance before installing
on the production service; monitor CPU/RSS of the main process and all
transcoder children and verify open/close/reopen of an SD channel.
