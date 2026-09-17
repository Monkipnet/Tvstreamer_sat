#include "protocols/outputs/GstSrtOutputProtocol.h"
#include "protocols/outputs/GstOutputProtocolUtils.h"
#include "protocols/GstProtocolTypes.h"
#include "protocols/SrtVpsProfile.h"

#include <iostream>

namespace tvs::protocols::outputs {

bool appendSrtSink(std::vector<std::string>& args, const StreamConfig& cfg, GstOutputSpec& spec) {
    appendMpegTsMux(args, cfg);

    // 203.55: the PCR-derived tsparse+double-clocksync chain produced a
    // repeatable pause/burst cadence on the already encoded MPEG-TS: direct
    // socket capture showed 100-160 ms no-data gaps while consecutive video
    // and audio PTS still advanced by exactly one frame.  Keep the mux PCR/PTS
    // untouched and pace CBR strictly from cumulative byte count instead.
    args.insert(args.end(), {
        "tsparse",
        "name=transcode_srt_ts_align",
        "set-timestamps=false",
        "alignment=7",
        "!"
    });
    appendCbrPacer(args, cfg, "transcode_srt_byte_cbr_pacer");
    appendOutputQueueWithTime(args, "transcode_srt_output_queue", 5000000000ULL, false);

    const std::string mode = srtOutputMode(cfg);
    const bool caller = mode == "caller";
    const int port = cfg.outputPort > 0 ? cfg.outputPort : 7001;

    if (!caller) {
        const uint16_t relayPort = transcodedSrtInternalPort(cfg);
        args.insert(args.end(), {
            "udpsink",
            "host=127.0.0.1",
            "port=" + std::to_string(relayPort),
            "sync=false",
            "async=false",
            "qos=false",
            "blocksize=1316",
            "buffer-size=8388608"
        });

        assignTsPads(cfg, spec);
        const std::string advertised = safeHost(cfg.outputHost, safeHost(cfg.interfaceAddress, "0.0.0.0"));
        spec.description = "srt-listener-relay@127.0.0.1:" + std::to_string(relayPort) +
                           "->srt://" + advertised + ":" + std::to_string(port);
        return true;
    }

    const std::string uri = tvs::protocols::srt_vps::applyToUri(
        "srt://" + safeHost(cfg.outputHost, "127.0.0.1") + ":" +
            std::to_string(port) + "?mode=caller", cfg);
    const int srtLatencyMs = tvs::protocols::srt_vps::latencyMs(cfg, 700);
    const int srtPollTimeoutMs = tvs::protocols::srt_vps::pollTimeoutMs(cfg, 5000);

    args.insert(args.end(), {
        "srtsink",
        "uri=" + uri,
        "latency=" + std::to_string(srtLatencyMs),
        "sync=false",
        "async=false",
        "qos=false",
        "max-lateness=-1",
        "blocksize=1316",
        "wait-for-connection=false",
        "poll-timeout=" + std::to_string(srtPollTimeoutMs)
    });

    if (!cfg.interfaceAddress.empty() && cfg.interfaceAddress != "0.0.0.0" && cfg.interfaceAddress != "::") {
        args.push_back("localaddress=" + cfg.interfaceAddress);
    }
    args.push_back("localport=0");

    if (cfg.srtVpsVdsOptimization) {
        std::cerr << "SRT VPS/VDS PROFILE 203.67: stream=" << cfg.id
                  << " direction=transcoded-output enabled=on"
                  << " latency_ms=" << srtLatencyMs
                  << " poll_timeout_ms=" << srtPollTimeoutMs
                  << " srt_rcvbuf=" << tvs::protocols::srt_vps::kSrtReceiveBufferBytes
                  << " srt_sndbuf=" << tvs::protocols::srt_vps::kSrtSendBufferBytes
                  << " fc_packets=" << tvs::protocols::srt_vps::kFlightWindowPackets
                  << " payload_size=" << tvs::protocols::srt_vps::kPayloadSizeBytes
                  << std::endl;
    }
    assignTsPads(cfg, spec);
    spec.description = "srt-caller@" + uri;
    return true;
}

} // namespace tvs::protocols::outputs
