#pragma once

#include <gst/gst.h>
#include <gio/gio.h>
#include <jsoncpp/json/json.h>
#include <array>
#include <atomic>
#include <map>
#include <set>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <vector>

#include "ConfigManager.h"
#include "UdpMediaDeliveryHealth.h"
#include "GstTranscoderProcess.h"
#include "TelegramNotifier.h"
#include "utils.h"

class MptsOutputManager;

struct RemapContext {
    inline static std::atomic<uint64_t> createdCount{0};
    inline static std::atomic<uint64_t> destroyedCount{0};

    RemapContext() { createdCount.fetch_add(1, std::memory_order_relaxed); }

    GstElement* mux = nullptr;
    GstElement* sink = nullptr;
    StreamConfig config;
    bool videoLinked = false;
    bool audioLinked = false;
    bool flvMux = false;
    bool rtspPush = false;
    bool hlsSink2 = false;
    // 203.05: restore the proven 202.74 HLS transport path. If hlsdemux
    // exposes complete MPEG-TS fragments, route them byte-for-byte through an
    // input-selector. Keep the elementary remux branch only as a fallback.
    GstElement* hlsInputSelector = nullptr;
    GstPad* hlsMuxSelectorPad = nullptr;
    GstPad* hlsDirectSelectorPad = nullptr;
    // 203.06: keep at most one compatibility tsdemux per HLS pipeline.
    // Pipeline owns the element; this is a non-owning pointer used to avoid
    // allocating another tsdemux on every transient HLS pad replacement.
    GstElement* hlsCompatTsDemux = nullptr;
    bool hlsDirectTsActive = false;
    bool programMapApplied = false;
    GstPad* preallocatedVideoMuxPad = nullptr;
    GstPad* preallocatedAudioMuxPad = nullptr;
    std::string videoPadName;
    std::string audioPadName;

    ~RemapContext() {
        destroyedCount.fetch_add(1, std::memory_order_relaxed);
        if (hlsMuxSelectorPad) gst_object_unref(hlsMuxSelectorPad);
        if (hlsDirectSelectorPad) gst_object_unref(hlsDirectSelectorPad);
        if (preallocatedVideoMuxPad) gst_object_unref(preallocatedVideoMuxPad);
        if (preallocatedAudioMuxPad) gst_object_unref(preallocatedAudioMuxPad);
    }
};

struct ActiveStreamSession {
    std::string streamId;
    std::string clientIp;
    std::string protocol;
    size_t connections = 0;
};


struct ExternalSrtOutputState {
    StreamConfig config;
    GstElement* pipeline = nullptr;
    GstBus* bus = nullptr;
    std::thread busThread;
};

struct SharedDvbFrontendState {
    std::string tuningSignature;
    GstElement* pipeline = nullptr;
    GstElement* source = nullptr;
    GstBus* bus = nullptr;
    size_t consumers = 0;
    std::map<std::string, std::set<uint16_t>> consumerPids;
    std::string requestedPids;
    bool fullTsCapture = false;
    // v178: one in-process dispatcher owns TS framing and PID routing for all
    // services on this physical frontend.  Kept opaque here so the hot-path
    // implementation stays local to StreamManager.cpp.
    std::shared_ptr<void> dispatcherState;
};

struct DvbServiceRelayState {
    // v178 no longer creates a per-service GStreamer full-MPTS relay.  The
    // shared dispatcher emits the already-selected SPTS to this localhost UDP
    // port and the normal stream pipeline consumes it.
    uint16_t outputPort = 0;
    std::shared_ptr<void> dispatcherConsumer;
};

// 203.66: per-NETUP-branch telemetry is collected after the monotonic
// reservoir pacer, immediately before the network output queue.  The older
// common output probe can sit before per-output remux/pacer branches when a
// stream has multiple outputs, so it cannot prove what bytes actually leave a
// particular SRT/HTTP branch.
struct FinalNetupTsHealth {
    std::size_t branchIndex = 0;
    std::string outputType;
    std::atomic<uint64_t> totalPackets{0};
    std::atomic<uint64_t> nullPackets{0};
    std::atomic<uint64_t> mediaPackets{0};
    std::atomic<uint64_t> patPackets{0};
    std::atomic<uint64_t> pmtPackets{0};
    std::atomic<uint64_t> pcrPackets{0};
    std::atomic<uint64_t> lastPacketNs{0};
    std::atomic<uint64_t> lastMediaNs{0};
    std::atomic<uint64_t> lastPatNs{0};
    std::atomic<uint64_t> lastPmtNs{0};
    std::atomic<uint64_t> lastPcrNs{0};
};

struct StreamState {
    std::atomic<bool> active{false};
    std::atomic<bool> running{false};
    bool usingBackup = false;
    bool backupAttempted = false;
    bool primaryRetryPending = false;
    bool inputLossNotified = false;
    std::string statusMessage = "stopped";
    std::string primaryInputUri;
    std::string activeInputUri;
    GstElement* pipeline = nullptr;
    // An idle private HTTP preview must not feed another full remux chain.
    // A shared counter also survives an HTTP relay thread finishing after a
    // stopped stream has been removed from StreamManager::streams.
    std::shared_ptr<std::atomic<uint32_t>> privatePreviewDemand =
        std::make_shared<std::atomic<uint32_t>>(0);
    GstBus* bus = nullptr;
    std::thread busThread;
    StreamConfig config;
    // runtimeConfig may point at the internal shared-DVB SPTS relay while
    // config always remains the user-visible/original stream configuration.
    StreamConfig runtimeConfig;
    bool sharedDvbInput = false;
    std::string sharedDvbFrontendKey;
    std::string sharedDvbServiceRelayUri;
    std::string sharedDvbServicePids;
    bool dvbTsRemapApplied = false;
    std::unique_ptr<DvbServiceRelayState> dvbServiceRelay;
    // Runtime PAT result used only when input_service_id=0 (Auto).
    // The configured value remains 0; effective selection is kept in state->config.
    std::atomic<uint64_t> inputBitrate{0};
    std::atomic<uint64_t> outputBitrate{0};
    std::atomic<uint64_t> inputBytes{0};
    std::atomic<uint64_t> outputBytes{0};
    std::atomic<uint64_t> stableUdpNetworkBytes{0};
    std::atomic<bool> stableUdpNetworkTelemetry{false};
    std::atomic<uint64_t> inputCcErrors{0};
    std::atomic<uint64_t> inputCcErrorsDelta{0};
    std::atomic<uint64_t> outputCcErrors{0};
    std::atomic<uint64_t> outputCcErrorsDelta{0};
    // Passive MPEG-TS scrambling telemetry for the web tile. These counters
    // only inspect transport_scrambling_control on output packets; they never
    // carry ECM/CW/key material and do not modify the transport path.
    std::atomic<uint64_t> outputTsPayloadPackets{0};
    std::atomic<uint64_t> outputTsScrambledPackets{0};
    std::atomic<uint64_t> outputTsClearPesStarts{0};
    std::atomic<uint64_t> outputTsPayloadPacketsDelta{0};
    std::atomic<uint64_t> outputTsScrambledPacketsDelta{0};
    std::atomic<uint64_t> outputTsClearPesStartsDelta{0};
    std::chrono::steady_clock::time_point lastBitrateSample = std::chrono::steady_clock::now();
    uint64_t lastInputBytesSample = 0;
    uint64_t lastOutputBytesSample = 0;
    uint64_t lastStableUdpNetworkBytesSample = 0;
    uint64_t lastInputCcErrorsSample = 0;
    uint64_t lastOutputCcErrorsSample = 0;
    uint64_t lastOutputTsPayloadPacketsSample = 0;
    uint64_t lastOutputTsScrambledPacketsSample = 0;
    uint64_t lastOutputTsClearPesStartsSample = 0;
    uint64_t lastInputBytesSeen = 0;
    std::chrono::steady_clock::time_point lastInputActivity = std::chrono::steady_clock::now();
    std::chrono::steady_clock::time_point lastPrimaryRetry = std::chrono::steady_clock::now();
    std::chrono::steady_clock::time_point lastSrtStatsLog = std::chrono::steady_clock::now();
    // 202.63: auto-CBR observes the incoming TS independently of the web/UI
    // bitrate sampler. It only raises the configured CBR after several
    // consecutive one-second windows above the current target; it never
    // automatically lowers a user/configured target.
    bool autoCbrSampleInitialized = false;
    uint64_t autoCbrLastInputBytes = 0;
    std::chrono::steady_clock::time_point autoCbrLastSample = std::chrono::steady_clock::now();
    unsigned autoCbrExcessSamples = 0;
    uint64_t autoCbrPeakBitrate = 0;
    std::chrono::steady_clock::time_point autoCbrLastRaise =
        std::chrono::steady_clock::time_point::min();
    // 202.61: serialize source-only network recovery. After a fresh SRT start
    // or a source-only reconnect, the 6-second detector must not immediately
    // schedule another recovery while the transport is still starting.
    std::atomic<bool> networkSourceReconnectInFlight{false};
    // Fresh-pipeline startup grace and source-reconnect grace are deliberately
    // separate. 202.61 reused one deadline for both states; a later startup/
    // rebuild arm could leave a source reconnect reported as permanently
    // in-flight. The reconnect deadline is owned only by source-only recovery.
    std::chrono::steady_clock::time_point networkRecoveryGraceUntil =
        std::chrono::steady_clock::time_point::min();
    std::chrono::steady_clock::time_point networkSourceReconnectDeadline =
        std::chrono::steady_clock::time_point::min();
    // 202.44: live HLS transport errors/EOS are recoverable. Keep the monitor
    // thread alive and schedule a controlled pipeline rebuild instead of
    // marking the whole channel stopped on a transient segment/HTTP failure.
    bool hlsRecoveryPending = false;
    unsigned hlsRecoveryAttempts = 0;
    std::chrono::steady_clock::time_point hlsRecoveryDue =
        std::chrono::steady_clock::time_point::min();
    // 202.66: after a successful HLS pipeline rebuild, give hlsdemux enough
    // time to reload the playlist/segments before another ERROR/EOS/no-input
    // is allowed to schedule a new rebuild. This prevents the 5-second rebuild
    // storm seen on a temporarily unavailable HLS origin.
    std::chrono::steady_clock::time_point hlsRecoveryGraceUntil =
        std::chrono::steady_clock::time_point::min();
    uint64_t hlsRecoverySuppressed = 0;
    // v200: overload recovery watchdog.  It uses raw TS byte/continuity counters
    // rather than CPU percentage, so it detects the actual damage caused by a
    // scheduling stall.  Recovery is armed while errors are occurring and is
    // executed only after the TS has been clean again for a short settle time.
    uint64_t overloadWatchInputBytes = 0;
    uint64_t overloadWatchOutputBytes = 0;
    uint64_t overloadWatchInputCcErrors = 0;
    uint64_t overloadWatchOutputCcErrors = 0;
    unsigned overloadBadWindows = 0;
    bool overloadRecoveryArmed = false;
    std::chrono::steady_clock::time_point overloadWatchSample = std::chrono::steady_clock::now();
    std::chrono::steady_clock::time_point overloadDamageLastSeen = std::chrono::steady_clock::time_point::min();
    std::chrono::steady_clock::time_point lastOverloadRecovery = std::chrono::steady_clock::time_point::min();
    std::array<uint8_t, 8192> inputContinuity {};
    std::array<bool, 8192> inputContinuityValid {};
    std::vector<uint8_t> inputTsRemainder;
    // 202.53: reuse one scratch buffer per telemetry path instead of allocating
    // a temporary vector for every GstBuffer callback. This keeps glibc tcache
    // and per-thread malloc arenas from retaining thousands of hot-path chunks.
    std::vector<uint8_t> inputTsScratch;
    std::mutex inputContinuityMutex;
    // 203.64: transport bytes are not proof of useful media. SRT-CBR can
    // remain perfectly alive while carrying only NULL/PSI after an upstream
    // UDP outage. Discover input audio/video PIDs from PAT/PMT and count
    // their packets independently so the watchdog can distinguish transport
    // activity from real media progress.
    std::atomic<uint64_t> inputTsMediaPackets{0};
    std::atomic<uint64_t> inputTsNullPackets{0};
    std::atomic<uint64_t> inputTsMediaPesStarts{0};
    // Source-side PES PTS progress, independent of CBR NULL-padding or UI bitrate.
    std::atomic<uint64_t> inputTsVideoPtsAdvances{0};
    std::atomic<uint64_t> mediaPipelineGeneration{0};
    std::vector<uint8_t> inputMediaRemainder;
    std::vector<uint8_t> inputMediaScratch;
    std::mutex inputMediaMutex;
    uint16_t inputTelemetryPmtPid = 0x1FFF;
    std::array<bool, 8192> inputTelemetryMediaPids {};
    std::array<bool, 8192> inputTelemetryVideoPids {};
    std::array<bool, 8192> inputVideoPtsKnown {};
    std::array<uint64_t, 8192> inputVideoPts90k {};
    bool inputTelemetryMediaPidsKnown = false;
    uint64_t lastInputMediaPacketsSeen = 0;
    uint64_t lastOutputMediaPacketsSeen = 0;
    std::chrono::steady_clock::time_point lastInputMediaActivity = std::chrono::steady_clock::now();
    std::chrono::steady_clock::time_point lastOutputMediaActivity = std::chrono::steady_clock::now();
    // 203.66: one health object per strict NETUP SRT/HTTP branch.  A shared_ptr
    // lets an old pipeline probe finish safely after a rebuild while the state
    // immediately publishes only the new generation's branch objects.
    std::mutex finalNetupTsHealthMutex;
    std::vector<std::shared_ptr<FinalNetupTsHealth>> finalNetupTsHealth;
    // Actual successful UDP datagram delivery, one object per UDP output.
    // Old senders may finish after their generation is retired; shared_ptr
    // keeps their counters alive without making them visible as current.
    std::mutex udpMediaHealthMutex;
    std::vector<std::shared_ptr<UdpMediaDeliveryHealth>> udpMediaHealth;
    std::array<uint8_t, 8192> outputContinuity {};
    std::array<bool, 8192> outputContinuityValid {};
    std::vector<uint8_t> outputTsRemainder;
    std::vector<uint8_t> outputTsScratch;
    std::mutex outputContinuityMutex;
    std::vector<uint8_t> outputScramblingRemainder;
    std::vector<uint8_t> outputScramblingScratch;
    std::mutex outputScramblingMutex;
    // Media PID discovery for the decode indicator. Clear PSI/ECM/teletext
    // packets must never be enough to report a channel as decoded.
    uint16_t outputTelemetryPmtPid = 0x1FFF;
    std::array<bool, 8192> outputTelemetryMediaPids {};
    bool outputTelemetryMediaPidsKnown = false;
    // v186: custom single-request libcurl -> appsrc HTTP MPEG-TS source.
    // Kept opaque here so curl/GStreamer implementation stays in StreamManager.cpp.
    std::shared_ptr<void> httpMpegTsInputState;
    // Non-owning tap target. The manager lives for the whole StreamManager
    // lifetime and receives only copies of the normalized outgoing SPTS.
    MptsOutputManager* mptsOutputManager = nullptr;
    std::unique_ptr<RemapContext> sourceContext;
    std::unique_ptr<GstTranscoderProcess> gstTranscoder;
    std::vector<std::unique_ptr<ExternalSrtOutputState>> externalSrtOutputs;
    std::vector<std::unique_ptr<RemapContext>> outputContexts;
};

class StreamManager {
public:
    explicit StreamManager(ConfigManager& cfg, TelegramNotifier& notifier);
    ~StreamManager();

    bool startStream(const StreamConfig& streamConfig, std::string* error = nullptr);
    bool restartStream(const StreamConfig& streamConfig, std::string* error = nullptr);
    bool stopStream(const std::string& id);
    bool stopStreamAsync(const std::string& id);
    void stopAll();
    bool isStreamActive(const std::string& id);
    std::vector<std::string> activeStreams();
    std::map<std::string, StreamState*> snapshot();
    bool addHttpClient(const std::string& id, int fd, const std::string& clientIp,
                       const std::string& previewSession = {});
    bool closePreviewSession(const std::string& id, const std::string& previewSession);
    bool addStreamSession(const std::string& streamId, const std::string& clientIp, const std::string& protocol);
    bool removeStreamSession(const std::string& streamId, const std::string& clientIp, const std::string& protocol);
    size_t activeHttpSessions(const std::string& clientIp) const;
    size_t activeSubscriberSessions(const SubscriberConfig& subscriber);
    std::vector<ActiveStreamSession> activeStreamSessions();
    size_t resetHttpSessions(const std::string& clientIp);
    size_t enforceSubscriberAccess();
    size_t restartSrtOutputsForStreams(const std::vector<std::string>& streamIds);
    size_t restartAllSrtOutputs();

    // Separate packet-level MPTS aggregator. These calls do not rebuild or
    // alter the normal per-channel pipelines.
    void configureMptsOutputs();
    bool startMptsOutput(const std::string& id, std::string* error = nullptr);
    bool stopMptsOutput(const std::string& id);
    Json::Value mptsSnapshot() const;
    Json::Value queueMemorySnapshot() const;

private:
    bool gstreamerInitialized;
    std::string buildPipelineDescription(const StreamConfig& cfg);
    GstElement* createPipeline(StreamState* state);
    GstElement* createSourceChain(StreamState* state, GstElement* pipeline, GstElement*& terminalElement);
    GstElement* createTestPatternChain(const StreamConfig& cfg, GstElement* pipeline, GstElement*& terminalElement);
    bool buildOutputBranches(StreamState* state, GstElement* pipeline, GstElement* sourceTail);
    bool buildOutputBranch(StreamState* state, GstElement* pipeline, GstElement* sourceTail, const StreamConfig& outputConfig, size_t branchIndex);
    bool buildPassthroughPipeline(StreamState* state, GstElement* pipeline, GstElement* sourceTail, const StreamConfig& outputConfig, size_t branchIndex);
    bool buildRemapPipeline(StreamState* state, GstElement* pipeline, GstElement* sourceTail, const StreamConfig& outputConfig, size_t branchIndex);
    bool buildRtmpOutputPipeline(StreamState* state, GstElement* pipeline, GstElement* sourceTail, const StreamConfig& outputConfig, size_t branchIndex);
    bool buildHlsOutputPipeline(StreamState* state, GstElement* pipeline, GstElement* sourceTail, const StreamConfig& outputConfig, size_t branchIndex);
    bool buildRtspOutputPipeline(StreamState* state, GstElement* pipeline, GstElement* sourceTail, const StreamConfig& outputConfig, size_t branchIndex);
    GstElement* createOutputSink(StreamState* state, const StreamConfig& cfg, GstElement* pipeline, const std::string& sinkName);
    GstElement* createExternalSrtOutputPipeline(const StreamConfig& cfg, std::string& error);
    bool startExternalSrtOutputs(StreamState* state, std::string& error);
    void stopExternalSrtOutputs(StreamState* state);
    bool restartPipelineWithInput(StreamState* state, const std::string& inputUri, bool useBackup);
    bool restartTranscodedInput(StreamState* state, const std::string& inputUri, bool useBackup);
    bool restartActiveInput(StreamState* state, const std::string& inputUri, bool useBackup);
    bool probeInputAvailable(const StreamConfig& baseConfig, const std::string& inputUri, std::chrono::milliseconds timeout);
    bool prepareSharedDvbInput(StreamState* state, std::string& error);
    void releaseSharedDvbInput(StreamState* state);
    bool acquireSharedDvbFrontend(StreamState* state, std::string& error);
    void releaseSharedDvbFrontend(StreamState* state);
    bool startDvbServiceRelay(StreamState* state, std::string& error);
    void stopDvbServiceRelay(StreamState* state);
    bool cleanupStreamState(const std::string& id, bool notifyManualStop);
    bool waitForStreamTeardown(const std::string& id, std::chrono::milliseconds timeout, std::string* error = nullptr);
    bool teardownStreamState(std::unique_ptr<StreamState> statePtr, const std::string& id,
                             const StreamConfig& stoppedConfig, bool notifyManualStop);
    void finishStreamTeardown(const std::string& id, uint64_t teardownToken);
    uint64_t reserveStreamTeardownLocked(const std::string& id);
    uint16_t allocateDvbServiceRelayPort(const std::string& streamId);
    void releaseDvbServiceRelayPort(uint16_t port);
    void notifyStreamState(const StreamConfig& cfg, const std::string& color, const std::string& title, const std::string& details);
    static void onDemuxPadAdded(GstElement* demux, GstPad* pad, gpointer user_data);
    static void onFlvDemuxPadAdded(GstElement* demux, GstPad* pad, gpointer user_data);
    static void onRtspPadAdded(GstElement* src, GstPad* pad, gpointer user_data);
    void monitorBus(const std::string& id);
    void monitorExternalSrtBus(const std::string& id, size_t outputIndex);
    GstElement* createTranscodedUdpRelayPipeline(StreamState* state, std::string& error);
    void attachBitrateProbes(StreamState* state);
    void updateBitrateEstimates(StreamState* state);
    void maybeAutoRaiseUdpCbr(StreamState* state, std::chrono::steady_clock::time_point now);
    bool applyAutoRaisedUdpCbr(StreamState* state, uint64_t measuredBitrate, uint64_t newTargetBitrate);
    static GstPadProbeReturn inputPadProbe(GstPad* pad, GstPadProbeInfo* info, gpointer user_data);
    static GstPadProbeReturn outputPadProbe(GstPad* pad, GstPadProbeInfo* info, gpointer user_data);
    bool isClientAllowedForStream(const std::string& streamId, const std::string& clientIp) const;
    void attachSrtConnectionMonitoring(GstElement* sink, const StreamConfig& cfg);
    void pruneExpiredAdHocSessionsLocked(std::chrono::steady_clock::time_point now);
    size_t disconnectHttpRelaySessionsForStream(const std::string& streamId, const char* reason);
    static gboolean onSrtCallerConnecting(GstElement* sink, GSocketAddress* addr, const gchar* streamId, gpointer userData);
    static void onSrtCallerAdded(GstElement* sink, gint, GSocketAddress* addr, gpointer userData);
    static void onSrtCallerRemoved(GstElement* sink, gint, GSocketAddress* addr, gpointer userData);
    static void onSrtCallerRejected(GstElement* sink, GSocketAddress* addr, const gchar* streamId, gpointer userData);

    ConfigManager& configManager;
    TelegramNotifier& telegramNotifier;
    std::map<std::string, std::unique_ptr<StreamState>> streams;
    // 202.60: a stream removed from `streams` is not immediately safe to start
    // again. Keep its id reserved until the old GStreamer pipeline has reached
    // NULL, all sender/remap state is released and the pipeline GObject is
    // actually finalized. This closes the stop/start race exposed by 202.59.
    std::set<std::string> stoppingStreamIds;
    // 202.66: teardown generations prevent a late completion from an old
    // stop worker from releasing the barrier of a newer stop. A timed-out old
    // generation is never force-retired; systemd replaces the process instead.
    std::map<std::string, uint64_t> stoppingStreamTokens;
    uint64_t nextStreamTeardownToken = 0;
    std::set<std::string> startingStreamIds;
    std::condition_variable streamLifecycleCondition;
    std::atomic<uint64_t> streamStartWaitCount{0};
    std::atomic<uint64_t> streamStartWaitTimeoutCount{0};
    std::atomic<uint64_t> streamFinalizeTimeoutCount{0};
    // Retained for diagnostics/backward compatibility. 202.66 never increments
    // this counter because a retained generation is not allowed to start over.
    std::atomic<uint64_t> streamForcedRetireCount{0};
    std::atomic<uint64_t> hlsRecoveryRebuildCount{0};
    std::atomic<uint64_t> hlsRecoverySuppressedCount{0};
    std::map<std::string, std::unique_ptr<SharedDvbFrontendState>> sharedDvbFrontends;
    std::unique_ptr<MptsOutputManager> mptsOutputManager;
    // When the last consumer stops, frontend shutdown happens on the stop
    // thread. Keep a short release barrier so a new transponder cannot race
    // the old GstDvbSrc while the kernel device is still closing.
    std::set<std::string> releasingDvbFrontends;
    std::condition_variable dvbReleaseCondition;
    std::set<uint16_t> dvbServiceRelayPorts;
    struct HttpClientSession {
        std::string streamId;
        std::string clientIp;
        std::string protocol;
        std::chrono::steady_clock::time_point lastActivity = std::chrono::steady_clock::now();
        // Local tcpserversink side of the detached HTTP relay. The relay thread
        // owns close(); lifecycle/recovery code only shutdown()s this descriptor
        // to wake a thread blocked on read when the stream pipeline is rebuilt.
        int upstreamFd = -1;
        std::string previewSession;  // non-empty only for authenticated private preview
    };
    std::map<int, HttpClientSession> httpClients;
    // Cancel-before-connect guard, expires automatically (also bounds memory).
    std::map<std::pair<std::string, std::string>, std::chrono::steady_clock::time_point>
        cancelledPreviewSessions;
    std::map<std::string, HttpClientSession> adHocSessions;
    mutable std::mutex managerMutex;
    // Serialize automatic target updates/config writes coming from independent
    // per-stream monitor threads.
    std::mutex autoCbrConfigMutex;
    std::atomic<uint64_t> nextSessionId{0};
    static void onHttpClientFdRemoved(GstElement* sink, gint fd, gpointer userData);
};
