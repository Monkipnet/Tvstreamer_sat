#include "NetworkTsInput.h"
#include "HlsSegmentScheduler.h"

#include "StreamManager.h"
#include "protocols/inputs/GstHlsInputProtocol.h"
#include "protocols/inputs/GstHttpInputProtocol.h"
#include "protocols/inputs/GstSrtInputProtocol.h"
#include "protocols/stream/StreamInputProtocol.h"
#include "protocols/SrtVpsProfile.h"
#include "utils.h"

#include <algorithm>
#include <iostream>
#include <string>

namespace {

// 202.57: restore the proven TVStreamer5 SRT/HTTP input timing.  The
// later 8-second SAT5 queue changed the dynamics of the original reservoir
// controller and did not remove the freezes.
// 202.72: enlarge only the bounded network ingest reservoirs. Keep them below
// the stream-level recovery windows so buffering absorbs short jitter/outages
// without hiding a genuinely dead source.
constexpr guint64 kNetworkInputQueue = 6 * GST_SECOND;
constexpr guint64 kHlsInputQueue = 10 * GST_SECOND;
constexpr gint kNetworkSourceTimeoutSeconds = 15;
// TVStreamer5/main uses 500 ms SRT latency for this path.
constexpr gint kSrtLatencyMs = 500;
constexpr gint kSrtPollTimeoutMs = 1000;
constexpr guint kNetworkQueueHardMaxBytes = 64U * 1024U * 1024U;
constexpr guint kHlsQueueHardMaxBytes = 40U * 1024U * 1024U;

bool hasElementFactory(const char* name) {
    GstElementFactory* factory = gst_element_factory_find(name);
    if (!factory) return false;
    gst_object_unref(factory);
    return true;
}

bool hasProperty(GstElement* element, const char* propertyName) {
    return element &&
        g_object_class_find_property(G_OBJECT_GET_CLASS(element), propertyName) != nullptr;
}

void setBooleanPropertyIfPresent(GstElement* element, const char* propertyName, gboolean value) {
    if (hasProperty(element, propertyName)) g_object_set(element, propertyName, value, nullptr);
}

void setIntPropertyIfPresent(GstElement* element, const char* propertyName, gint value) {
    if (hasProperty(element, propertyName)) g_object_set(element, propertyName, value, nullptr);
}

void setDoublePropertyIfPresent(GstElement* element, const char* propertyName, gdouble value) {
    if (hasProperty(element, propertyName)) g_object_set(element, propertyName, value, nullptr);
}

void setUIntPropertyIfPresent(GstElement* element, const char* propertyName, guint value) {
    if (hasProperty(element, propertyName)) g_object_set(element, propertyName, value, nullptr);
}

void setStringPropertyIfPresent(
    GstElement* element, const char* propertyName, const std::string& value) {
    if (hasProperty(element, propertyName) && !value.empty()) {
        g_object_set(element, propertyName, value.c_str(), nullptr);
    }
}

bool addElementOrFail(GstElement* pipeline, GstElement* element) {
    return pipeline && element && gst_bin_add(GST_BIN(pipeline), element);
}

GstElement* addQueue(
    GstElement* pipeline, const char* name, guint64 maxSizeTime, guint maxSizeBytes) {
    GstElement* queue = gst_element_factory_make("queue", name);
    if (!queue || !addElementOrFail(pipeline, queue)) {
        if (queue && !GST_OBJECT_PARENT(queue)) gst_object_unref(queue);
        return nullptr;
    }
    g_object_set(queue,
        "max-size-buffers", 0,
        // 202.46: never rely on buffer timestamps as the only queue bound.
        // MPEG-TS buffers can temporarily have missing/irregular duration during
        // reconnect/remap; a time-only queue can then retain far more memory than
        // the configured queue window.
        "max-size-bytes", maxSizeBytes,
        "max-size-time", maxSizeTime,
        "leaky", 0,
        nullptr);
    return queue;
}

std::string configuredInputInterfaceAddress(const StreamConfig& cfg) {
    return cfg.inputInterfaceAddressConfigured
        ? cfg.inputInterfaceAddress
        : cfg.interfaceAddress;
}

std::string appendAccessQuery(const std::string& uri, const StreamConfig& cfg) {
    if (cfg.hlsAccessKeyMode != "query" || cfg.hlsAccessKeyName.empty() ||
        cfg.hlsAccessKeyValue.empty()) {
        return uri;
    }

    gchar* escapedName = g_uri_escape_string(cfg.hlsAccessKeyName.c_str(), nullptr, TRUE);
    gchar* escapedValue = g_uri_escape_string(cfg.hlsAccessKeyValue.c_str(), nullptr, TRUE);
    if (!escapedName || !escapedValue) {
        if (escapedName) g_free(escapedName);
        if (escapedValue) g_free(escapedValue);
        return uri;
    }

    const std::string keyPrefix = std::string(escapedName) + "=";
    const auto queryPos = uri.find('?');
    if (queryPos != std::string::npos) {
        const std::string query = uri.substr(queryPos + 1);
        if (query.rfind(keyPrefix, 0) == 0 ||
            query.find("&" + keyPrefix) != std::string::npos) {
            g_free(escapedName);
            g_free(escapedValue);
            return uri;
        }
    }

    const std::string result = uri + (queryPos == std::string::npos ? "?" : "&") +
        escapedName + "=" + escapedValue;
    g_free(escapedName);
    g_free(escapedValue);
    return result;
}

void configureHttpCredentials(GstElement* element, const StreamConfig& cfg) {
    if (!element) return;
    GstElementFactory* factory = gst_element_get_factory(element);
    const gchar* factoryName = factory
        ? gst_plugin_feature_get_name(GST_PLUGIN_FEATURE(factory))
        : nullptr;
    if (!factoryName ||
        (g_strcmp0(factoryName, "souphttpsrc") != 0 &&
         g_strcmp0(factoryName, "curlhttpsrc") != 0)) {
        return;
    }

    setStringPropertyIfPresent(element, "user-agent", cfg.hlsUserAgent);
    setBooleanPropertyIfPresent(element, "keep-alive", TRUE);
    setIntPropertyIfPresent(element, "timeout", kNetworkSourceTimeoutSeconds);

    if (cfg.hlsAccessKeyMode == "header" && !cfg.hlsAccessKeyName.empty() &&
        !cfg.hlsAccessKeyValue.empty() && hasProperty(element, "extra-headers")) {
        GstStructure* headers = gst_structure_new_empty("extra-headers");
        gst_structure_set(headers,
            cfg.hlsAccessKeyName.c_str(), G_TYPE_STRING, cfg.hlsAccessKeyValue.c_str(),
            nullptr);
        g_object_set(element, "extra-headers", headers, nullptr);
        gst_structure_free(headers);
    }
}

void onHlsChildLocationChanged(GObject* object, GParamSpec*, gpointer userData) {
    auto* cfg = static_cast<StreamConfig*>(userData);
    if (!cfg || cfg->hlsAccessKeyMode != "query" || cfg->hlsAccessKeyValue.empty()) return;
    if (g_object_get_data(object, "tvs-network-hls-query-update")) return;

    gchar* current = nullptr;
    g_object_get(object, "location", &current, nullptr);
    if (!current || !*current) {
        if (current) g_free(current);
        return;
    }

    const std::string updated = appendAccessQuery(current, *cfg);
    if (updated != current) {
        g_object_set_data(object, "tvs-network-hls-query-update", GINT_TO_POINTER(1));
        g_object_set(object, "location", updated.c_str(), nullptr);
        g_object_set_data(object, "tvs-network-hls-query-update", nullptr);
    }
    g_free(current);
}

void configureHlsChildSource(GstElement* element, StreamConfig& cfg) {
    if (!element) return;
    GstElementFactory* factory = gst_element_get_factory(element);
    const gchar* factoryName = factory
        ? gst_plugin_feature_get_name(GST_PLUGIN_FEATURE(factory))
        : nullptr;
    if (!factoryName ||
        (g_strcmp0(factoryName, "souphttpsrc") != 0 &&
         g_strcmp0(factoryName, "curlhttpsrc") != 0)) {
        return;
    }

    // 203.05: restore the 202.74 HLS child-source policy. Segment/playlist
    // fetches retry through transient HTTP failures; the stream-level watchdog
    // remains the final recovery owner if TS delivery really stops.
    configureHttpCredentials(element, cfg);
    setIntPropertyIfPresent(element, "retries", -1);
    setDoublePropertyIfPresent(element, "retry-backoff-factor", 0.25);
    setDoublePropertyIfPresent(element, "retry-backoff-max", 2.0);
    if (cfg.hlsAccessKeyMode == "query" && hasProperty(element, "location")) {
        if (!g_object_get_data(G_OBJECT(element), "tvs-network-hls-location-watch")) {
            g_signal_connect(element, "notify::location",
                G_CALLBACK(onHlsChildLocationChanged), &cfg);
            g_object_set_data(
                G_OBJECT(element), "tvs-network-hls-location-watch", GINT_TO_POINTER(1));
        }
        onHlsChildLocationChanged(G_OBJECT(element), nullptr, &cfg);
    }
}

void onHlsDeepElementAdded(GstBin*, GstBin*, GstElement* element, gpointer userData) {
    auto* ctx = static_cast<RemapContext*>(userData);
    if (!ctx) return;
    configureHlsChildSource(element, ctx->config);
}

GstElement* buildSrt(
    const StreamConfig& cfg,
    GstElement* pipeline,
    GstElement*& terminalElement,
    std::string& error) {
    const std::string mode = toLower(cfg.inputMode) == "listener" ? "listener" : "caller";
    const char* preferredFactory = mode == "listener" ? "srtsrc" : "srtclientsrc";
    const char* factory = preferredFactory;

    // TVStreamer5 uses srtclientsrc for caller mode.  Keep a compatibility
    // fallback for older GStreamer installations which expose only srtsrc.
    if (!hasElementFactory(factory)) {
        if (mode == "caller" && hasElementFactory("srtsrc")) {
            factory = "srtsrc";
            std::cerr << "Network TS input 202.28: srtclientsrc unavailable; "
                      << "falling back to srtsrc caller mode" << std::endl;
        } else {
            error = std::string("missing element: ") + preferredFactory;
            return nullptr;
        }
    }

    GstElement* src = gst_element_factory_make(factory, "input_src");
    GstElement* queue = addQueue(pipeline, "input_queue", kNetworkInputQueue, kNetworkQueueHardMaxBytes);
    if (!src || !queue || !addElementOrFail(pipeline, src)) {
        if (src && !GST_OBJECT_PARENT(src)) gst_object_unref(src);
        error = "SRT input: failed to create srtsrc/srtclientsrc or queue";
        return nullptr;
    }

    const std::string uri = tvs::protocols::inputs::srtInputUri(cfg);
    g_object_set(src, "uri", uri.c_str(), nullptr);
    setBooleanPropertyIfPresent(src, "do-timestamp", TRUE);
    const bool hasAutoReconnect = hasProperty(src, "auto-reconnect");
    setBooleanPropertyIfPresent(src, "auto-reconnect", TRUE);
    const gint effectiveLatencyMs = tvs::protocols::srt_vps::latencyMs(cfg, kSrtLatencyMs);
    const gint effectivePollTimeoutMs = tvs::protocols::srt_vps::pollTimeoutMs(cfg, kSrtPollTimeoutMs);
    setIntPropertyIfPresent(src, "latency", effectiveLatencyMs);
    // 202.63: older GStreamer SRT sources default poll-timeout to -1. A source
    // stuck in an infinite libsrt poll can then block gst_element_set_state(NULL)
    // and prevent the application recovery deadline from ever running. Keep the
    // media latency at 500 ms, but bound the control-path poll to one second.
    setIntPropertyIfPresent(src, "poll-timeout", effectivePollTimeoutMs);
    setStringPropertyIfPresent(src, "localaddress", configuredInputInterfaceAddress(cfg));

    if (mode == "listener") {
        setIntPropertyIfPresent(src, "mode", 2);
        setBooleanPropertyIfPresent(src, "wait-for-connection", TRUE);
        setBooleanPropertyIfPresent(src, "keep-listening", TRUE);
    } else {
        setIntPropertyIfPresent(src, "mode", 1);
        setBooleanPropertyIfPresent(src, "wait-for-connection", FALSE);
        setUIntPropertyIfPresent(src, "localport", 0);
    }

    if (!gst_element_link(src, queue)) {
        error = "SRT input: failed to link source -> queue";
        return nullptr;
    }

    terminalElement = queue;
    std::cerr << "Network TS input 202.72: protocol=SRT mode=" << mode
              << " factory=" << factory
              << " latency_ms=" << effectiveLatencyMs
              << " poll_timeout_ms=" << effectivePollTimeoutMs
              << " auto_reconnect_property=" << (hasAutoReconnect ? "yes" : "no")
              << " app_reconnect=203.48-source-only-first+full-fallback"
              << " queue_ms=6000 queue_max_mb=64 leaky=off prebuffer=off"
              << " do_timestamp=on input_pacing=off" << std::endl;
    if (cfg.srtVpsVdsOptimization) {
        std::cerr << "SRT VPS/VDS PROFILE 203.67: stream=" << cfg.id
                  << " direction=input enabled=on"
                  << " latency_ms=" << tvs::protocols::srt_vps::kLatencyMs
                  << " rcvlatency_ms=" << tvs::protocols::srt_vps::kLatencyMs
                  << " peerlatency_ms=" << tvs::protocols::srt_vps::kLatencyMs
                  << " poll_timeout_ms=" << tvs::protocols::srt_vps::kPollTimeoutMs
                  << " srt_rcvbuf=" << tvs::protocols::srt_vps::kSrtReceiveBufferBytes
                  << " srt_sndbuf=" << tvs::protocols::srt_vps::kSrtSendBufferBytes
                  << " fc_packets=" << tvs::protocols::srt_vps::kFlightWindowPackets
                  << " payload_size=" << tvs::protocols::srt_vps::kPayloadSizeBytes
                  << " note=kernel-udp-buffer-remains-host-controlled"
                  << std::endl;
    }
    return src;
}

GstElement* buildHttp(
    const StreamConfig& cfg,
    GstElement* pipeline,
    GstElement*& terminalElement,
    std::string& error) {
    if (!hasElementFactory("souphttpsrc")) {
        error = "missing element: souphttpsrc";
        return nullptr;
    }

    GstElement* src = gst_element_factory_make("souphttpsrc", "input_src");
    GstElement* queue = addQueue(pipeline, "input_queue", kNetworkInputQueue, kNetworkQueueHardMaxBytes);
    if (!src || !queue || !addElementOrFail(pipeline, src)) {
        if (src && !GST_OBJECT_PARENT(src)) gst_object_unref(src);
        error = "HTTP MPEG-TS input: failed to create souphttpsrc/queue";
        return nullptr;
    }

    const std::string location = appendAccessQuery(
        tvs::protocols::inputs::httpInputUri(cfg), cfg);
    g_object_set(src,
        "location", location.c_str(),
        "is-live", TRUE,
        "do-timestamp", TRUE,
        nullptr);
    configureHttpCredentials(src, cfg);
    setBooleanPropertyIfPresent(src, "compress", FALSE);
    // 202.57: TVStreamer5/main leaves souphttpsrc retry policy at its normal
    // GStreamer setting.  Do not run a second infinite retry/backoff loop inside
    // the source while StreamManager already owns reconnect/recovery policy.

    const std::string inputInterface = configuredInputInterfaceAddress(cfg);
    if (!inputInterface.empty()) {
        std::cerr << "HTTP MPEG-TS input: input_iface=" << inputInterface
                  << " selected; souphttpsrc follows the kernel HTTP route" << std::endl;
    }

    if (!gst_element_link(src, queue)) {
        error = "HTTP MPEG-TS input: failed to link souphttpsrc -> queue";
        return nullptr;
    }

    terminalElement = queue;
    std::cerr << "Network TS input 202.72: protocol=HTTP transport=souphttpsrc direct_queue=on capsfilter=off"
              << " queue_ms=6000 queue_max_mb=64 leaky=off prebuffer=off"
              << " do_timestamp=on libcurl_appsrc=off input_pacing=off"
              << " http_retries=gstreamer-default recovery=watchdog+error+eos"
              << " access=" << (cfg.hlsAccessKeyMode.empty() ? "none" : cfg.hlsAccessKeyMode)
              << std::endl;
    return src;
}

GstElement* buildHls(
    StreamState* state,
    GstElement* pipeline,
    GstElement*& terminalElement,
    GCallback hlsPadAddedCallback,
    tvs::network_input::ConfigureTsMuxFn configureTsMux,
    std::string& error) {
    (void)hlsPadAddedCallback;
    (void)configureTsMux;
    if (!state) {
        error = "HLS input: missing stream state";
        return nullptr;
    }
    if (!hasElementFactory("appsrc")) {
        error = "HLS input: missing appsrc";
        return nullptr;
    }

    // 203.18: duration-controlled HLS downloader with provider TS timing.  Do not let hlsdemux
    // prefetch an arbitrary number of complete segments.  The scheduler fetches
    // each segment at full HTTP speed only when downstream consumed-duration
    // falls below the low watermark, then stops again at the target/high water.
    // MPEG-TS payload bytes are preserved byte-for-byte.
    StreamConfig& cfg = state->runtimeConfig;
    GstElement* src = gst_element_factory_make("appsrc", "input_hls_scheduler_src");
    GstElement* queue = addQueue(
        pipeline, "input_queue", kHlsInputQueue, kHlsQueueHardMaxBytes);
    if (!src || !queue || !addElementOrFail(pipeline, src)) {
        if (src && !GST_OBJECT_PARENT(src)) gst_object_unref(src);
        error = "HLS input: failed to create duration scheduler appsrc/queue";
        return nullptr;
    }

    GstCaps* caps = gst_caps_new_simple(
        "video/mpegts",
        "systemstream", G_TYPE_BOOLEAN, TRUE,
        "packetsize", G_TYPE_INT, 188,
        nullptr);
    g_object_set(src,
        "is-live", TRUE,
        "format", GST_FORMAT_TIME,
        "block", TRUE,
        "do-timestamp", FALSE,
        "caps", caps,
        nullptr);
    gst_caps_unref(caps);

    // Bound appsrc itself as a final safety valve.  The actual download policy
    // is duration based and normally stays well below these limits.
    if (hasProperty(src, "max-bytes")) {
        g_object_set(src, "max-bytes", static_cast<guint64>(16ULL * 1024ULL * 1024ULL), nullptr);
    }
    if (hasProperty(src, "max-time")) {
        g_object_set(src, "max-time", static_cast<guint64>(12ULL * GST_SECOND), nullptr);
    }

    if (!gst_element_link(src, queue)) {
        error = "HLS input: failed to link scheduler appsrc -> queue";
        return nullptr;
    }

    auto* scheduler = new tvs::hls_scheduler::Scheduler(pipeline, src, queue, cfg);
    if (!scheduler->start(error)) {
        delete scheduler;
        return nullptr;
    }
    g_object_set_data_full(
        G_OBJECT(pipeline), tvs::hls_scheduler::kPipelineDataKey, scheduler,
        [](gpointer data) {
            delete static_cast<tvs::hls_scheduler::Scheduler*>(data);
        });

    terminalElement = queue;
    std::cerr << "Network TS input 203.25: protocol=HLS"
              << " source=duration-controlled-segment-scheduler+appsrc"
              << " low_ahead_ms=6000 target_ahead_ms=8000 high_ahead_ms=12000"
              << " min_start_segments=2 segment_fetch=full-speed-on-demand"
              << " queue_ms=10000 queue_max_mb=40 leaky=off"
              << " direct_mpegts=always remux=off hlsdemux=off input_pacing=segment-demand"
              << " gst_buffer_timestamps=unset provider_pcr_pts=authoritative"
              << " watchdog_rebuild_ms=15000 buffered_ahead_guard=on"
              << std::endl;
    return src;
}
} // namespace

namespace tvs::network_input {

bool handles(const StreamConfig& cfg) {
    const auto kind = tvs::stream_protocols::inputKind(cfg);
    return kind == tvs::stream_protocols::InputProtocolKind::Srt ||
           kind == tvs::stream_protocols::InputProtocolKind::Http ||
           kind == tvs::stream_protocols::InputProtocolKind::Hls;
}

GstElement* build(
    StreamState* state,
    GstElement* pipeline,
    GstElement*& terminalElement,
    GCallback hlsPadAddedCallback,
    ConfigureTsMuxFn configureTsMux,
    std::string& error) {
    terminalElement = nullptr;
    if (!state || !pipeline) {
        error = "Network TS input: invalid stream state/pipeline";
        return nullptr;
    }

    const StreamConfig& cfg = state->runtimeConfig;
    const auto kind = tvs::stream_protocols::inputKind(cfg);
    switch (kind) {
        case tvs::stream_protocols::InputProtocolKind::Srt:
            return buildSrt(cfg, pipeline, terminalElement, error);
        case tvs::stream_protocols::InputProtocolKind::Http:
            return buildHttp(cfg, pipeline, terminalElement, error);
        case tvs::stream_protocols::InputProtocolKind::Hls:
            return buildHls(
                state, pipeline, terminalElement,
                hlsPadAddedCallback, configureTsMux, error);
        default:
            error = "Network TS input: unsupported protocol";
            return nullptr;
    }
}

} // namespace tvs::network_input
