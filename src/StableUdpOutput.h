#pragma once

#include <gst/gst.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include "UdpMediaDeliveryHealth.h"
#include <string>

#include "ConfigManager.h"

namespace StableUdpOutput {

struct MemoryStats {
    uint64_t packetRingCapacityBytes = 0;
    uint64_t senderCount = 0;
    uint64_t queuedChunkPayloadBytes = 0;
    uint64_t queuedChunkCapacityBytes = 0;
    uint64_t queuedChunkCount = 0;
    uint64_t queuedChunkMaxCapacityBytes = 0;
    uint64_t inputRemainderCapacityBytes = 0;
    uint64_t cleanStartCapacityBytes = 0;
    uint64_t senderCreated = 0;
    uint64_t senderDestroyed = 0;
};

MemoryStats memoryStats();

// 202.63: raise the live target of every CBR StableUDP sender belonging to
// a stream without rebuilding the pipeline. Returns the number of updated
// sender instances; the caller persists the stream-level target separately.
std::size_t raiseCbrTargetBitrate(const std::string& streamId, uint64_t bitrate);
// Highest current media-rate estimate among StableUDP senders for this stream.
// Unlike socket arrival bitrate, HLS 203.23 uses scheduler segment bytes/EXTINF
// duration as its primary media clock, with legacy PTS/PCR only as startup fallback.
uint64_t maxInputBitrateEstimate(const std::string& streamId);

GstElement* createSink(
    GstElement* pipeline,
    const StreamConfig& config,
    const std::string& sinkName,
    std::string& error,
    std::atomic<uint64_t>* networkBytes = nullptr,
    std::shared_ptr<UdpMediaDeliveryHealth> mediaHealth = nullptr);

} // namespace StableUdpOutput
