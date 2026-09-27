#include "protocols/outputs/GstHlsOutputProtocol.h"
#include "protocols/outputs/GstOutputProtocolUtils.h"
#include "protocols/GstProtocolTypes.h"

#include <algorithm>
#include <cctype>
#include <filesystem>
#include <chrono>
#include <unistd.h>

namespace tvs::protocols::outputs {

namespace {

uint32_t hlsVideoPid(const StreamConfig& cfg) {
    return cfg.videoPid > 0 ? cfg.videoPid : 258;
}

uint32_t hlsAudioPid(const StreamConfig& cfg) {
    return cfg.audioPid > 0 ? cfg.audioPid : 257;
}

uint32_t hlsServiceId(const StreamConfig& cfg) {
    return std::max<uint32_t>(cfg.serviceId, 1);
}

std::string hlsPublicPathName(const StreamConfig& cfg) {
    const std::string raw = !cfg.name.empty() ? cfg.name : (!cfg.serviceName.empty() ? cfg.serviceName : cfg.id);
    std::string result;
    bool underscore = false;
    for (unsigned char ch : raw) {
        if (std::isalnum(ch) || ch == '-' || ch == '_') {
            result.push_back(static_cast<char>(ch));
            underscore = false;
        } else if (!result.empty() && !underscore) {
            result.push_back('_');
            underscore = true;
        }
    }
    while (!result.empty() && result.back() == '_') result.pop_back();
    if (result.empty()) {
        for (unsigned char ch : cfg.id) {
            if (std::isalnum(ch) || ch == '-' || ch == '_') result.push_back(static_cast<char>(ch));
        }
    }
    return result.empty() ? "stream" : result;
}

std::string prepareHlsDirectory(const StreamConfig& cfg) {
    std::filesystem::path dir = cfg.hlsArchiveEnabled
        ? (std::filesystem::path(cfg.hlsArchivePath) / cfg.id)
        : (std::filesystem::path("/tmp/tvstreammersat5-hls") / cfg.id);
    if (!cfg.hlsVariantName.empty()) {
        dir /= "abr";
        dir /= cfg.hlsVariantName;
    }

    std::error_code ec;
    if (!cfg.hlsArchiveEnabled) {
        std::filesystem::remove_all(dir, ec);
        ec.clear();
    }
    std::filesystem::create_directories(dir, ec);
    if (cfg.hlsArchiveEnabled && !ec) {
        const auto cutoff = std::chrono::system_clock::now() - std::chrono::hours(cfg.hlsArchiveHours);
        for (const auto& entry : std::filesystem::directory_iterator(dir, ec)) {
            if (ec) break;
            if (!entry.is_regular_file() || entry.path().extension() != ".ts") continue;
            const auto ft = entry.last_write_time(ec);
            if (ec) { ec.clear(); continue; }
            const auto st = std::chrono::time_point_cast<std::chrono::system_clock::duration>(
                ft - std::filesystem::file_time_type::clock::now() + std::chrono::system_clock::now());
            if (st < cutoff) std::filesystem::remove(entry.path(), ec);
            ec.clear();
        }
    }
    return dir.string();
}
std::string hlsSegmentPattern(const StreamConfig& cfg, const std::string& dir) {
    if (!cfg.hlsArchiveEnabled) return dir + "/segment%05d.ts";
    const auto epoch = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    return dir + "/segment" + std::to_string(epoch) + "_" + std::to_string(::getpid()) + "_%05d.ts";
}

void appendHlsMux(std::vector<std::string>& args, const StreamConfig& cfg, GstOutputSpec& spec) {
    const uint32_t videoPid = hlsVideoPid(cfg);
    const uint32_t audioPid = hlsAudioPid(cfg);
    const uint32_t serviceId = hlsServiceId(cfg);

    args.insert(args.end(), {
        "mpegtsmux",
        "name=hls_mux",
        "alignment=7",
        "pat-interval=9000",
        "pmt-interval=9000",
        "pcr-interval=1800",
        "si-interval=9000",
        "bitrate=" + std::to_string(transportCbrEnabled(cfg) ? muxBitrate(cfg) : 0)
    });

    // When CBR is enabled for HLS, NULL-packet stuffing keeps the MPEG-TS
    // segment transport rate deterministic. With CBR disabled the mux stays VBR.

    if (cfg.remapEnabled) {
        // mpegtsmux uses the numeric suffix of a requested sink_<PID> pad as
        // the elementary-stream PID. prog-map then puts both PIDs into the
        // configured MPEG-TS service/program.
        args.push_back(
            "prog-map=program_map,sink_" + std::to_string(videoPid) + "=" + std::to_string(serviceId) +
            ",sink_" + std::to_string(audioPid) + "=" + std::to_string(serviceId));
        spec.videoPad = "hls_mux.sink_" + std::to_string(videoPid);
        spec.audioPad = "hls_mux.sink_" + std::to_string(audioPid);
    } else {
        spec.videoPad = "hls_mux.";
        spec.audioPad = "hls_mux.";
    }

    args.push_back("!");
}

} // namespace

bool appendHlsSink(std::vector<std::string>& args, const StreamConfig& cfg, GstOutputSpec& spec) {
    // HLS gets its own mux instance and explicit request pads instead of the
    // generic mux helper. This keeps PID/service remapping deterministic for
    // every newly generated segment.
    appendHlsMux(args, cfg, spec);
    // 202.79: HLS is file segmentation, not a wall-clock network transport.
    // Re-timestamping the already muxed TS with tsparse set-timestamps=true can
    // slowly move PCR relative to AAC PTS and manifests as periodic audio gaps.
    // Preserve mux timestamps and use tsparse only for packet alignment.
    args.insert(args.end(), {"tsparse", "name=transcode_hls_ts_align", "set-timestamps=false", "alignment=7", "!"});
    appendOutputQueueWithTime(args, "transcode_hls_output_queue", 12000000000ULL, false);

    // Remove an old playlist and stale .ts files before starting a new HLS
    // generation. Otherwise a client can briefly read segments from the
    // previous pipeline, including their old PID map.
    const std::string dir = prepareHlsDirectory(cfg);
    args.insert(args.end(), {
        "hlssink",
        "playlist-location=" + dir + "/video.m3u8",
        "location=" + hlsSegmentPattern(cfg, dir),
        "playlist-root=/" + hlsPublicPathName(cfg) + "/" +
            (cfg.hlsVariantName.empty() ? std::string() : "abr/" + cfg.hlsVariantName + "/"),
        "target-duration=2",
        "max-files=" + std::to_string(cfg.hlsArchiveEnabled ? std::max<uint32_t>(9u, cfg.hlsArchiveHours * 1800u + 60u) : 9u),
        "playlist-length=7"
    });

    spec.description = "hls@" + dir + "/video.m3u8";
    return true;
}

} // namespace tvs::protocols::outputs
