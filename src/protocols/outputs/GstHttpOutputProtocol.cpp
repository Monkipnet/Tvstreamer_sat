#include "protocols/outputs/GstHttpOutputProtocol.h"
#include "protocols/outputs/GstOutputProtocolUtils.h"
#include "protocols/GstProtocolTypes.h"
namespace tvs::protocols::outputs {
bool appendHttpSink(std::vector<std::string>& args, const StreamConfig& cfg, GstOutputSpec& spec) {
    appendMpegTsMux(args, cfg);

    // 203.55: do not derive a second wall clock from mux PCR and then gate it
    // through two clocksync elements.  RAW TCP capture proved that path emitted
    // correct 20.0/21.3 ms A/V PTS in 100-160 ms wall-clock bursts.  Preserve
    // MPEG-TS timestamps and pace CBR from byte count at muxBitrate()/8.
    args.insert(args.end(), {
        "tsparse",
        "name=transcode_http_ts_align",
        "set-timestamps=false",
        "alignment=7",
        "!"
    });
    appendCbrPacer(args, cfg, "transcode_http_byte_cbr_pacer");
    const bool privatePreview = cfg.outputPort == 0 && cfg.outputHost == "127.0.0.1";
    appendOutputQueueWithTime(args, "transcode_http_output_queue",
                              privatePreview ? 1000000000ULL : 8000000000ULL,
                              privatePreview);
    const int internalPort = static_cast<int>(transcodedHttpInternalPort(cfg));
    args.insert(args.end(), {
        "tcpserversink",
        "host=127.0.0.1",
        "port=" + std::to_string(internalPort),
        "sync=false",
        "async=false",
        "qos=false"
    });
    assignTsPads(cfg, spec);
    spec.description = "http-ts@http-port:" + std::to_string(cfg.outputPort) + "->tcp://127.0.0.1:" + std::to_string(internalPort);
    return true;
}
}
