#pragma once

#include "TsMediaProgress.h"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <string>

// 203.70: packet observations are made ONLY after a successful sendto() in
// StableUdpOutput. The configured bitrate, PCR, PAT/PMT and PID 0x1fff cannot
// advance any of the media/video counters.
struct UdpMediaDeliveryHealth {
    std::string outputEndpoint;
    std::atomic<uint64_t> sentDatagrams{0};
    std::atomic<uint64_t> mediaPackets{0};
    std::atomic<uint64_t> videoPesStarts{0};
    std::atomic<uint64_t> videoPtsAdvances{0};
};

namespace tvs::udp_media_delivery {

struct PacketProgress {
    uint64_t mediaPackets = 0;
    uint64_t videoPesStarts = 0;
    uint64_t videoPtsAdvances = 0;
};

// Single-sender, per-generation PTS history. No heap allocation or logging in
// the UDP send thread. A zero/unknown PID never guesses media from non-NULL TS.
inline PacketProgress inspectSuccessfulDatagram(
    const uint8_t* bytes, std::size_t size, uint16_t videoPid,
    uint16_t audioPid, bool& ptsKnown, uint64_t& previousPts) {
    PacketProgress progress;
    if (!bytes || size % 188 != 0 || videoPid >= 0x1fff || audioPid >= 0x1fff) {
        return progress;
    }
    for (std::size_t offset = 0; offset < size; offset += 188) {
        const uint8_t* packet = bytes + offset;
        if (packet[0] != 0x47 || (packet[1] & 0x80U)) continue;
        const uint16_t pid = static_cast<uint16_t>(((packet[1] & 0x1fU) << 8) | packet[2]);
        if (pid == 0x1fff ||
            ((videoPid == 0 || pid != videoPid) &&
             (audioPid == 0 || pid != audioPid))) continue;
        const uint8_t control = static_cast<uint8_t>((packet[3] >> 4) & 3U);
        if (control != 1 && control != 3) continue;
        const std::size_t payload = control == 1 ? 4U : 5U + packet[4];
        if (payload >= 188) continue;
        ++progress.mediaPackets;
        if (videoPid != 0 && pid == videoPid && (packet[1] & 0x40U)) {
            uint64_t pts = 0;
            if (tvs::ts_media_progress::videoPesPts90k(packet, 188, pts)) {
                ++progress.videoPesStarts;
                if (ptsKnown && previousPts != pts) ++progress.videoPtsAdvances;
                previousPts = pts;
                ptsKnown = true;
            }
        }
    }
    return progress;
}

// Pure policy used by the monitor and unit tests. A sender which only pads
// its transport with NULL/PSI/PCR has no mediaEstablished progress. Do not
// recover from upstream starvation here; the source watchdog owns that case.
enum class Fault {
    None, NoMediaStarted, MediaStalled, VideoPtsStalled, VideoPtsNeverAdvanced
};

struct FaultEvidence {
    bool eligible = false;
    bool inputMediaRecent = false;
    bool inputVideoPtsAdvancing = false;
    bool outputMediaEstablished = false;
    bool outputVideoPtsEstablished = false;
    uint64_t outputVideoPesStarts = 0;
    int64_t startupAgeMs = 0;
    int64_t outputMediaGapMs = 0;
    int64_t outputVideoPtsGapMs = 0;
};

inline Fault classifyFault(const FaultEvidence& e) {
    if (!e.eligible || !e.inputMediaRecent || e.startupAgeMs < 20000) {
        return Fault::None;
    }
    if (!e.outputMediaEstablished) return Fault::NoMediaStarted;
    if (e.outputMediaGapMs >= 18000) return Fault::MediaStalled;
    if (e.inputVideoPtsAdvancing && e.outputVideoPtsEstablished &&
        e.outputVideoPtsGapMs >= 18000) return Fault::VideoPtsStalled;
    if (e.inputVideoPtsAdvancing && !e.outputVideoPtsEstablished &&
        e.outputVideoPesStarts >= 3) return Fault::VideoPtsNeverAdvanced;
    return Fault::None;
}

inline const char* faultReason(Fault fault) {
    switch (fault) {
    case Fault::NoMediaStarted: return "input-media-live-udp-media-never-started";
    case Fault::MediaStalled: return "input-media-live-udp-media-stalled";
    case Fault::VideoPtsStalled: return "input-video-pts-advancing-udp-video-pts-stalled";
    case Fault::VideoPtsNeverAdvanced: return "input-video-pts-advancing-udp-video-pts-never-advanced";
    default: return nullptr;
    }
}

} // namespace tvs::udp_media_delivery
