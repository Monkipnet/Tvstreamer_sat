#pragma once

#include "ConfigManager.h"

#include <array>
#include <string>
#include <utility>

namespace tvs::protocols::srt_vps {

// 203.67: optional global SRT profile for VPS/VDS/container hosts where the
// kernel UDP socket limits and scheduler jitter are outside TVStreammerSAT5's
// control.  Keep this opt-in so physical/LAN installations retain their proven
// lower-latency defaults.
inline constexpr int kLatencyMs = 1500;
inline constexpr int kPollTimeoutMs = 2000;
inline constexpr int kSrtReceiveBufferBytes = 16 * 1024 * 1024;
inline constexpr int kSrtSendBufferBytes = 16 * 1024 * 1024;
inline constexpr int kFlightWindowPackets = 32768;
inline constexpr int kPayloadSizeBytes = 1316;

inline std::string setQueryParameter(
    std::string uri, const std::string& key, const std::string& value) {
    const std::string needle = key + "=";
    const std::size_t queryPos = uri.find('?');
    if (queryPos == std::string::npos) {
        uri += "?" + needle + value;
        return uri;
    }

    std::size_t pos = queryPos + 1;
    while (pos <= uri.size()) {
        const std::size_t end = uri.find('&', pos);
        const std::size_t tokenEnd = end == std::string::npos ? uri.size() : end;
        const std::size_t eq = uri.find('=', pos);
        if (eq != std::string::npos && eq < tokenEnd && uri.compare(pos, eq - pos, key) == 0) {
            uri.replace(eq + 1, tokenEnd - (eq + 1), value);
            return uri;
        }
        if (end == std::string::npos) break;
        pos = end + 1;
    }

    if (!uri.empty() && uri.back() != '?' && uri.back() != '&') uri += '&';
    uri += needle + value;
    return uri;
}

inline std::string applyToUri(std::string uri, const StreamConfig& cfg) {
    if (!cfg.srtVpsVdsOptimization) return uri;

    const std::array<std::pair<const char*, int>, 8> options {{
        {"latency", kLatencyMs},
        {"rcvlatency", kLatencyMs},
        {"peerlatency", kLatencyMs},
        {"rcvbuf", kSrtReceiveBufferBytes},
        {"sndbuf", kSrtSendBufferBytes},
        {"fc", kFlightWindowPackets},
        {"payloadsize", kPayloadSizeBytes},
        {"poll-timeout", kPollTimeoutMs},
    }};
    for (const auto& [name, number] : options) {
        uri = setQueryParameter(uri, name, std::to_string(number));
    }
    return uri;
}

inline int latencyMs(const StreamConfig& cfg, int normalValue) {
    return cfg.srtVpsVdsOptimization ? kLatencyMs : normalValue;
}

inline int pollTimeoutMs(const StreamConfig& cfg, int normalValue) {
    return cfg.srtVpsVdsOptimization ? kPollTimeoutMs : normalValue;
}

} // namespace tvs::protocols::srt_vps
