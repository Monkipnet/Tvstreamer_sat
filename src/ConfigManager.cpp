#include "ConfigManager.h"
#include "utils.h"

#include <algorithm>
#include <fstream>
#include <iostream>
#include <sstream>
#include <chrono>
#include <ctime>
#include <iomanip>
#include <array>
#include <vector>
#include <set>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <openssl/evp.h>
#include <openssl/rand.h>


namespace {

bool usesUdpEndpointFields(const std::string& outputType) {
    const std::string type = toLower(outputType);
    return type == "udp" || type == "udp-cbr" || type == "udp-vbr" || type == "rtp";
}

void normalizeOutputEndpoint(std::string& outputHost, int& outputPort, const std::string& outputType) {
    if (!usesUdpEndpointFields(outputType)) return;
    std::string normalizedHost;
    int normalizedPort = outputPort;
    if (normalizeUdpEndpoint(outputHost, outputPort, normalizedHost, normalizedPort)) {
        outputHost = normalizedHost;
        outputPort = normalizedPort;
    }
}

constexpr const char* kUiPasswordPrefix = "enc:v1:";
constexpr size_t kUiPasswordKeyBytes = 32;
constexpr size_t kUiPasswordNonceBytes = 12;
constexpr size_t kUiPasswordTagBytes = 16;

std::filesystem::path uiPasswordKeyPath(const std::filesystem::path& configPath) {
    return configPath.parent_path() / "tvstreammersat5-ui.key";
}

std::string hexEncode(const unsigned char* data, size_t size) {
    static constexpr char digits[] = "0123456789abcdef";
    std::string out;
    out.resize(size * 2);
    for (size_t i = 0; i < size; ++i) {
        out[i * 2] = digits[(data[i] >> 4) & 0x0F];
        out[i * 2 + 1] = digits[data[i] & 0x0F];
    }
    return out;
}

int hexNibble(char ch) {
    if (ch >= '0' && ch <= '9') return ch - '0';
    if (ch >= 'a' && ch <= 'f') return ch - 'a' + 10;
    if (ch >= 'A' && ch <= 'F') return ch - 'A' + 10;
    return -1;
}

bool hexDecode(const std::string& text, std::vector<unsigned char>& out) {
    if ((text.size() & 1U) != 0) return false;
    out.resize(text.size() / 2);
    for (size_t i = 0; i < out.size(); ++i) {
        const int hi = hexNibble(text[i * 2]);
        const int lo = hexNibble(text[i * 2 + 1]);
        if (hi < 0 || lo < 0) return false;
        out[i] = static_cast<unsigned char>((hi << 4) | lo);
    }
    return true;
}

bool readUiPasswordKey(
    const std::filesystem::path& path,
    std::array<unsigned char, kUiPasswordKeyBytes>& key) {
    std::ifstream input(path, std::ios::binary);
    if (!input.is_open()) return false;
    input.read(reinterpret_cast<char*>(key.data()), static_cast<std::streamsize>(key.size()));
    if (input.gcount() != static_cast<std::streamsize>(key.size())) return false;
    char extra = 0;
    if (input.read(&extra, 1)) return false;
    ::chmod(path.c_str(), S_IRUSR | S_IWUSR);
    return true;
}

bool createUiPasswordKey(
    const std::filesystem::path& path,
    std::array<unsigned char, kUiPasswordKeyBytes>& key) {
    if (RAND_bytes(key.data(), static_cast<int>(key.size())) != 1) {
        std::cerr << "Unable to generate UI password encryption key" << std::endl;
        return false;
    }

    const int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_EXCL, S_IRUSR | S_IWUSR);
    if (fd < 0) {
        if (errno == EEXIST) return readUiPasswordKey(path, key);
        std::cerr << "Unable to create UI password key " << path
                  << ": " << std::strerror(errno) << std::endl;
        return false;
    }

    size_t offset = 0;
    while (offset < key.size()) {
        const ssize_t written = ::write(fd, key.data() + offset, key.size() - offset);
        if (written < 0) {
            if (errno == EINTR) continue;
            std::cerr << "Unable to write UI password key " << path
                      << ": " << std::strerror(errno) << std::endl;
            ::close(fd);
            ::unlink(path.c_str());
            return false;
        }
        offset += static_cast<size_t>(written);
    }
    ::fsync(fd);
    ::close(fd);
    ::chmod(path.c_str(), S_IRUSR | S_IWUSR);
    return true;
}

bool loadUiPasswordKey(
    const std::filesystem::path& configPath,
    bool allowCreate,
    std::array<unsigned char, kUiPasswordKeyBytes>& key) {
    const auto path = uiPasswordKeyPath(configPath);
    if (readUiPasswordKey(path, key)) return true;
    if (!allowCreate) {
        std::cerr << "UI password is encrypted but key file is missing or invalid: "
                  << path << std::endl;
        return false;
    }
    return createUiPasswordKey(path, key);
}

bool encryptUiPassword(
    const std::filesystem::path& configPath,
    const std::string& plaintext,
    std::string& encoded) {
    std::array<unsigned char, kUiPasswordKeyBytes> key{};
    if (!loadUiPasswordKey(configPath, true, key)) return false;

    std::array<unsigned char, kUiPasswordNonceBytes> nonce{};
    std::array<unsigned char, kUiPasswordTagBytes> tag{};
    if (RAND_bytes(nonce.data(), static_cast<int>(nonce.size())) != 1) return false;

    std::vector<unsigned char> ciphertext(plaintext.size() + 16);
    int produced = 0;
    int total = 0;
    EVP_CIPHER_CTX* ctx = EVP_CIPHER_CTX_new();
    if (!ctx) return false;
    bool ok =
        EVP_EncryptInit_ex(ctx, EVP_aes_256_gcm(), nullptr, nullptr, nullptr) == 1 &&
        EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_SET_IVLEN, static_cast<int>(nonce.size()), nullptr) == 1 &&
        EVP_EncryptInit_ex(ctx, nullptr, nullptr, key.data(), nonce.data()) == 1 &&
        EVP_EncryptUpdate(ctx, ciphertext.data(), &produced,
            reinterpret_cast<const unsigned char*>(plaintext.data()),
            static_cast<int>(plaintext.size())) == 1;
    if (ok) {
        total = produced;
        ok = EVP_EncryptFinal_ex(ctx, ciphertext.data() + total, &produced) == 1;
        total += produced;
    }
    if (ok) {
        ok = EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_GET_TAG,
            static_cast<int>(tag.size()), tag.data()) == 1;
    }
    EVP_CIPHER_CTX_free(ctx);
    if (!ok) return false;
    ciphertext.resize(static_cast<size_t>(total));

    std::vector<unsigned char> blob;
    blob.reserve(nonce.size() + ciphertext.size() + tag.size());
    blob.insert(blob.end(), nonce.begin(), nonce.end());
    blob.insert(blob.end(), ciphertext.begin(), ciphertext.end());
    blob.insert(blob.end(), tag.begin(), tag.end());
    encoded = std::string(kUiPasswordPrefix) + hexEncode(blob.data(), blob.size());
    return true;
}

bool decryptUiPassword(
    const std::filesystem::path& configPath,
    const std::string& encoded,
    std::string& plaintext) {
    if (encoded.rfind(kUiPasswordPrefix, 0) != 0) {
        plaintext = encoded;
        return true;
    }

    std::array<unsigned char, kUiPasswordKeyBytes> key{};
    if (!loadUiPasswordKey(configPath, false, key)) return false;

    std::vector<unsigned char> blob;
    if (!hexDecode(encoded.substr(std::strlen(kUiPasswordPrefix)), blob) ||
        blob.size() < kUiPasswordNonceBytes + kUiPasswordTagBytes) {
        std::cerr << "Invalid encrypted UI password format" << std::endl;
        return false;
    }

    const unsigned char* nonce = blob.data();
    const size_t ciphertextSize = blob.size() - kUiPasswordNonceBytes - kUiPasswordTagBytes;
    const unsigned char* ciphertext = blob.data() + kUiPasswordNonceBytes;
    const unsigned char* tag = blob.data() + kUiPasswordNonceBytes + ciphertextSize;
    std::vector<unsigned char> output(ciphertextSize + 1);
    int produced = 0;
    int total = 0;

    EVP_CIPHER_CTX* ctx = EVP_CIPHER_CTX_new();
    if (!ctx) return false;
    bool ok =
        EVP_DecryptInit_ex(ctx, EVP_aes_256_gcm(), nullptr, nullptr, nullptr) == 1 &&
        EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_SET_IVLEN,
            static_cast<int>(kUiPasswordNonceBytes), nullptr) == 1 &&
        EVP_DecryptInit_ex(ctx, nullptr, nullptr, key.data(), nonce) == 1 &&
        EVP_DecryptUpdate(ctx, output.data(), &produced, ciphertext,
            static_cast<int>(ciphertextSize)) == 1;
    if (ok) {
        total = produced;
        ok = EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_SET_TAG,
            static_cast<int>(kUiPasswordTagBytes), const_cast<unsigned char*>(tag)) == 1;
    }
    if (ok) {
        ok = EVP_DecryptFinal_ex(ctx, output.data() + total, &produced) == 1;
        total += produced;
    }
    EVP_CIPHER_CTX_free(ctx);
    if (!ok) {
        std::cerr << "Unable to decrypt UI password: authentication tag mismatch" << std::endl;
        return false;
    }
    plaintext.assign(reinterpret_cast<const char*>(output.data()), static_cast<size_t>(total));
    return true;
}

} // namespace

StreamOutputConfig StreamOutputConfig::fromJson(const Json::Value& root) {
    StreamOutputConfig output;
    output.outputType = root.get("output_type", "udp-cbr").asString();
    output.outputMode = root.get("output_mode", "listener").asString();
    output.outputHost = root.get("output_host", "127.0.0.1").asString();
    output.outputPort = root.get("output_port", 1234).asInt();
    output.interfaceAddress = root.get("interface_address", "").asString();
    normalizeOutputEndpoint(output.outputHost, output.outputPort, output.outputType);
    if (toLower(output.outputType) == "srt" && (!root.isMember("output_port") || output.outputPort <= 0 || output.outputPort > 65535)) {
        output.outputPort = 7001;
    }
    return output;
}

Json::Value StreamOutputConfig::toJson() const {
    Json::Value root;
    root["output_type"] = outputType;
    root["output_mode"] = outputMode;
    root["output_host"] = outputHost;
    root["output_port"] = outputPort;
    root["interface_address"] = interfaceAddress;
    return root;
}

MptsServiceConfig MptsServiceConfig::fromJson(const Json::Value& root) {
    MptsServiceConfig service;
    service.streamId = root.get("stream_id", "").asString();
    service.serviceId = root.get("service_id", 0).asUInt();
    if (service.serviceId > 0xFFFFU) service.serviceId = 0;
    return service;
}

Json::Value MptsServiceConfig::toJson() const {
    Json::Value root;
    root["stream_id"] = streamId;
    root["service_id"] = serviceId;
    return root;
}

MptsOutputConfig MptsOutputConfig::fromJson(const Json::Value& root) {
    MptsOutputConfig output;
    output.id = root.get("id", "").asString();
    output.name = root.get("name", output.id).asString();
    output.outputHost = root.get("output_host", "239.255.20.1").asString();
    output.outputPort = root.get("output_port", 5000).asInt();
    // Accept the same convenient host syntax as normal UDP outputs.
    std::string normalizedHost;
    int normalizedPort = output.outputPort;
    if (normalizeUdpEndpoint(output.outputHost, output.outputPort, normalizedHost, normalizedPort)) {
        output.outputHost = normalizedHost;
        output.outputPort = normalizedPort;
    }
    if (output.outputPort <= 0 || output.outputPort > 65535) output.outputPort = 5000;
    output.interfaceAddress = root.get("interface_address", "").asString();
    output.autoStart = root.get("auto_start", false).asBool();
    output.transportStreamId = std::clamp(root.get("transport_stream_id", 1).asUInt(), 1U, 0xFFFFU);
    output.originalNetworkId = std::clamp(root.get("original_network_id", 1).asUInt(), 1U, 0xFFFFU);
    output.serviceIdBase = std::clamp(root.get("service_id_base", 1).asUInt(), 1U, 0xFFFFU);
    output.pmtPidBase = std::clamp(root.get("pmt_pid_base", 0x1000).asUInt(), 0x20U, 0x1FFEU);
    if (root.isMember("services") && root["services"].isArray()) {
        std::set<std::string> seen;
        for (const auto& item : root["services"]) {
            auto service = MptsServiceConfig::fromJson(item);
            if (!service.streamId.empty() && seen.insert(service.streamId).second) {
                output.services.push_back(std::move(service));
            }
        }
    } else if (root.isMember("stream_ids") && root["stream_ids"].isArray()) {
        std::set<std::string> seen;
        for (const auto& item : root["stream_ids"]) {
            MptsServiceConfig service;
            service.streamId = item.asString();
            if (!service.streamId.empty() && seen.insert(service.streamId).second) {
                output.services.push_back(std::move(service));
            }
        }
    }
    // The packet-level mapper reserves 32 PID slots per service below the PMT
    // range. Keep v202.42 bounded and deterministic rather than silently
    // creating overlapping PID blocks.
    if (output.services.size() > 120) output.services.resize(120);
    if (!output.services.empty()) {
        const uint32_t countMinusOne = static_cast<uint32_t>(output.services.size() - 1);
        output.pmtPidBase = std::min<uint32_t>(output.pmtPidBase, 0x1FFEU - countMinusOne);
        output.serviceIdBase = std::min<uint32_t>(output.serviceIdBase, 0xFFFFU - countMinusOne);
    }
    return output;
}

Json::Value MptsOutputConfig::toJson() const {
    Json::Value root;
    root["id"] = id;
    root["name"] = name;
    root["output_host"] = outputHost;
    root["output_port"] = outputPort;
    root["interface_address"] = interfaceAddress;
    root["auto_start"] = autoStart;
    root["transport_stream_id"] = transportStreamId;
    root["original_network_id"] = originalNetworkId;
    root["service_id_base"] = serviceIdBase;
    root["pmt_pid_base"] = pmtPidBase;
    Json::Value list(Json::arrayValue);
    for (const auto& service : services) list.append(service.toJson());
    root["services"] = list;
    return root;
}

StreamConfig StreamConfig::fromJson(const Json::Value& root) {
    StreamConfig config;
    config.id = root.get("id", "").asString();
    config.name = root.get("name", "").asString();
    config.inputUri = root.get("input_uri", "").asString();
    config.backupInputUri = root.get("backup_input_uri", "").asString();
    config.backupInputType = root.get("backup_input_type", "").asString();
    if (config.backupInputType.empty()) {
        const std::string backupUri = config.backupInputUri;
        config.backupInputType =
            backupUri.empty()
                ? "url"
                : (backupUri.rfind("file://", 0) == 0 || backupUri.find("://") == std::string::npos
                ? "file"
                : "url");
    }
    config.backupFileLoop = root.get("backup_file_loop", false).asBool();
    config.outputType = root.get("output_type", "udp-cbr").asString();
    config.outputMode = root.get("output_mode", "listener").asString();
    config.outputHost = root.get("output_host", "127.0.0.1").asString();
    config.outputPort = root.get("output_port", 1234).asInt();
    normalizeOutputEndpoint(config.outputHost, config.outputPort, config.outputType);
    if (toLower(config.outputType) == "srt" && (!root.isMember("output_port") || config.outputPort <= 0 || config.outputPort > 65535)) {
        config.outputPort = 7001;
    }
    config.interfaceAddress = root.get("interface_address", "").asString();
    config.inputInterfaceAddressConfigured = root.isMember("input_interface_address");
    config.inputInterfaceAddress = root.get("input_interface_address", "").asString();
    config.inputMode = root.get("input_mode", "auto").asString();
    config.hlsAccessKeyMode = root.get("hls_access_key_mode", "none").asString();
    if (config.hlsAccessKeyMode != "header" && config.hlsAccessKeyMode != "query") {
        config.hlsAccessKeyMode = "none";
    }
    config.hlsAccessKeyName = root.get("hls_access_key_name",
        config.hlsAccessKeyMode == "query" ? "token" : "Authorization").asString();
    config.hlsAccessKeyValue = root.get("hls_access_key_value", "").asString();
    config.hlsUserAgent = root.get("hls_user_agent", "Mozilla/5.0 TVStreammerSAT5").asString();
    config.hlsSlowPcrAssist = root.get("hls_slow_pcr_assist", false).asBool();
    config.hlsPcrPhasePacing = root.get("hls_pcr_phase_pacing", false).asBool();
    // The two manual HLS timing modes are mutually exclusive. Pre-buffered PCR
    // interval pacing wins if an old API client accidentally submits both flags.
    if (config.hlsPcrPhasePacing) config.hlsSlowPcrAssist = false;
    config.testPattern = root.get("test_pattern", false).asBool();
    config.autoStart = root.get("auto_start", false).asBool();
    config.remapEnabled = root.get("remap_enabled", false).asBool();
    config.cbr = root.get("cbr", true).asBool();
    config.targetBitrate = root.get("target_bitrate", Json::UInt64(2000000)).asUInt64();
    config.transcodeEnabled = root.get("transcode_enabled", false).asBool();
    config.transcodeResolution = root.get("transcode_resolution", "1920x1080").asString();
    config.transcodeVideoCodec = root.get("transcode_video_codec", "h264").asString();
    if (config.transcodeVideoCodec != "copy") config.transcodeVideoCodec = "h264";
    config.transcodeVideoEncoder = toLower(root.get("transcode_video_encoder", "auto").asString());
    if (config.transcodeVideoEncoder != "auto" && config.transcodeVideoEncoder != "x264" &&
        config.transcodeVideoEncoder != "nvenc" && config.transcodeVideoEncoder != "intel") {
        config.transcodeVideoEncoder = "auto";
    }
    config.transcodeVideoBitrate = root.get("transcode_video_bitrate", Json::UInt64(6000000)).asUInt64();
    config.hlsArchiveEnabled = root.get("hls_archive_enabled", false).asBool();
    config.hlsArchiveHours = std::clamp(root.get("hls_archive_hours", 24).asUInt(), 1u, 168u);
    config.hlsArchivePath = root.get("hls_archive_path", "/var/lib/tvstreammersat5/archive").asString();
    if (config.hlsArchivePath.empty() || config.hlsArchivePath.front() != '/') {
        config.hlsArchivePath = "/var/lib/tvstreammersat5/archive";
    }
    config.transcodeAudioCodec = root.get("transcode_audio_codec", "aac").asString();
    if (config.transcodeAudioCodec != "aac" && config.transcodeAudioCodec != "mp3" &&
        config.transcodeAudioCodec != "copy") {
        config.transcodeAudioCodec = "aac";
    }
    config.transcodeAudioBitrate = root.get("transcode_audio_bitrate", Json::UInt64(192000)).asUInt64();
    config.transcodeAudioBitrate = std::clamp<uint64_t>(config.transcodeAudioBitrate, 64000, 320000);
    config.audioPid = root.get("audio_pid", 0).asUInt();
    config.videoPid = root.get("video_pid", 0).asUInt();
    config.serviceId = root.get("service_id", 1).asUInt();
    // input_service_id=0 means automatic PAT-based service detection.
    // Existing configurations that predate input_service_id keep their previous
    // explicit behaviour by inheriting service_id; newly created streams use 0.
    config.inputServiceId = root.isMember("input_service_id")
        ? root.get("input_service_id", 0).asUInt()
        : config.serviceId;
    config.serviceName = root.get("service_name", "").asString();
    config.serviceProvider = root.get("service_provider", "").asString();
    config.conditionalAccessClient = root.get("conditional_access_client", "").asString();
    if (root.isMember("outputs") && root["outputs"].isArray() && root["outputs"].size() > 0) {
        const auto primary = StreamOutputConfig::fromJson(root["outputs"][0]);
        config.outputType = primary.outputType;
        config.outputMode = primary.outputMode;
        config.outputHost = primary.outputHost;
        config.outputPort = primary.outputPort;
        for (Json::ArrayIndex i = 1; i < root["outputs"].size(); ++i) {
            config.additionalOutputs.push_back(StreamOutputConfig::fromJson(root["outputs"][i]));
        }
    } else if (root.isMember("additional_outputs") && root["additional_outputs"].isArray()) {
        for (const auto& item : root["additional_outputs"]) {
            config.additionalOutputs.push_back(StreamOutputConfig::fromJson(item));
        }
    }
    return config;
}

Json::Value StreamConfig::toJson() const {
    Json::Value root;
    root["id"] = id;
    root["name"] = name;
    root["input_uri"] = inputUri;
    root["backup_input_uri"] = backupInputUri;
    root["backup_input_type"] = backupInputType;
    root["backup_file_loop"] = backupFileLoop;
    root["output_type"] = outputType;
    root["output_mode"] = outputMode;
    root["output_host"] = outputHost;
    root["output_port"] = outputPort;
    root["interface_address"] = interfaceAddress;
    if (inputInterfaceAddressConfigured) {
        root["input_interface_address"] = inputInterfaceAddress;
    }
    root["input_mode"] = inputMode;
    root["hls_access_key_mode"] = hlsAccessKeyMode;
    root["hls_access_key_name"] = hlsAccessKeyName;
    root["hls_access_key_value"] = hlsAccessKeyValue;
    root["hls_user_agent"] = hlsUserAgent;
    root["hls_slow_pcr_assist"] = hlsSlowPcrAssist;
    root["hls_pcr_phase_pacing"] = hlsPcrPhasePacing;
    root["test_pattern"] = testPattern;
    root["auto_start"] = autoStart;
    root["remap_enabled"] = remapEnabled;
    root["cbr"] = cbr;
    root["target_bitrate"] = Json::UInt64(targetBitrate);
    root["transcode_enabled"] = transcodeEnabled;
    root["transcode_resolution"] = transcodeResolution;
    root["transcode_video_codec"] = transcodeVideoCodec;
    root["transcode_video_encoder"] = transcodeVideoEncoder;
    root["transcode_video_bitrate"] = Json::UInt64(transcodeVideoBitrate);
    root["hls_archive_enabled"] = hlsArchiveEnabled;
    root["hls_archive_hours"] = hlsArchiveHours;
    root["hls_archive_path"] = hlsArchivePath;
    root["transcode_audio_codec"] = transcodeAudioCodec;
    root["transcode_audio_bitrate"] = Json::UInt64(transcodeAudioBitrate);
    root["audio_pid"] = audioPid;
    root["video_pid"] = videoPid;
    root["input_service_id"] = inputServiceId;
    root["service_id"] = serviceId;
    root["service_name"] = serviceName;
    root["service_provider"] = serviceProvider;
    root["conditional_access_client"] = conditionalAccessClient;
    Json::Value extraOutputs(Json::arrayValue);
    for (const auto& output : additionalOutputs) {
        extraOutputs.append(output.toJson());
    }
    root["additional_outputs"] = extraOutputs;
    return root;
}

CamClientConfig CamClientConfig::fromJson(const Json::Value& root) {
    CamClientConfig config;
    config.id = root.get("id", "").asString();
    config.name = root.get("name", config.id).asString();
    config.maxServices = std::clamp(root.get("max_services", 10).asUInt(), 1u, 64u);
    config.backendId = root.get("backend_id", "newcamd").asString();
    if (config.backendId.empty() || config.backendId == "passthrough") config.backendId = "newcamd";
    config.backendConfig = root.get("backend_config", "{}").asString();
    if (config.backendConfig.empty()) config.backendConfig = "{}";
    return config;
}

Json::Value CamClientConfig::toJson() const {
    Json::Value root;
    root["id"] = id;
    root["name"] = name.empty() ? id : name;
    root["max_services"] = std::clamp(maxServices, 1u, 64u);
    root["backend_id"] = backendId.empty() ? "newcamd" : backendId;
    root["backend_config"] = backendConfig.empty() ? "{}" : backendConfig;
    return root;
}
Json::Value AppConfig::toJson() const {
    Json::Value root;
    root["login"] = login;
    root["password"] = password;
    root["server_name"] = serverName;
    root["http_port"] = httpPort;
    root["language"] = language;
    root["telegram_token"] = telegramToken;
    root["telegram_chat_id"] = telegramChatId;
    Json::Value camClientsJson(Json::arrayValue);
    for (const auto& client : camClients) camClientsJson.append(client.toJson());
    root["cam_clients"] = camClientsJson;
    Json::Value list(Json::arrayValue);
    for (const auto& stream : streams) {
        list.append(stream.toJson());
    }
    root["streams"] = list;
    Json::Value mptsList(Json::arrayValue);
    for (const auto& output : mptsOutputs) mptsList.append(output.toJson());
    root["mpts_outputs"] = mptsList;
    return root;
}

AppConfig AppConfig::fromJson(const Json::Value& root) {
    AppConfig config;
    config.login = root.get("login", "admin").asString();
    config.password = root.get("password", "admin").asString();
    config.serverName = root.get("server_name", "TVStreammerSAT5").asString();
    config.httpPort = root.get("http_port", 9000).asInt();
    config.language = root.get("language", "en").asString();
    if (config.language != "ru" && config.language != "en") {
        config.language = "en";
    }
    config.telegramToken = root.get("telegram_token", "").asString();
    config.telegramChatId = root.get("telegram_chat_id", "").asString();
    if (root.isMember("cam_clients") && root["cam_clients"].isArray()) {
        for (const auto& item : root["cam_clients"]) config.camClients.push_back(CamClientConfig::fromJson(item));
    }
    if (root.isMember("streams") && root["streams"].isArray()) {
        for (const auto& item : root["streams"]) {
            config.streams.push_back(StreamConfig::fromJson(item));
        }
    }
    if (root.isMember("mpts_outputs") && root["mpts_outputs"].isArray()) {
        std::set<std::string> seen;
        for (const auto& item : root["mpts_outputs"]) {
            auto output = MptsOutputConfig::fromJson(item);
            if (!output.id.empty() && !output.services.empty() && seen.insert(output.id).second) {
                config.mptsOutputs.push_back(std::move(output));
            }
        }
    }
    return config;
}

ConfigManager::ConfigManager() {
    configPath = std::filesystem::current_path() / "tvstreammersat5-config.json";
}

Json::Value SubscriberConfig::toJson() const {
    Json::Value root;
    root["name"] = name;
    root["primary_ip"] = primaryIp;
    root["backup_ip"] = backupIp;
    root["added_at"] = addedAt;
    root["enabled"] = enabled;
    Json::Value streams(Json::arrayValue);
    for (const auto& id : streamIds) streams.append(id);
    root["stream_ids"] = streams;
    return root;
}

SubscriberConfig SubscriberConfig::fromJson(const Json::Value& root) {
    SubscriberConfig subscriber;
    subscriber.name = root.get("name", "").asString();
    subscriber.primaryIp = root.get("primary_ip", "").asString();
    subscriber.backupIp = root.get("backup_ip", "").asString();
    subscriber.addedAt = root.get("added_at", "").asString();
    subscriber.enabled = root.get("enabled", true).asBool();
    if (root["stream_ids"].isArray()) {
        for (const auto& id : root["stream_ids"]) {
            subscriber.streamIds.push_back(id.asString());
        }
    }
    return subscriber;
}

Json::Value SubscriberListConfig::toJson() const {
    Json::Value root;
    root["filtering_enabled"] = filteringEnabled;
    Json::Value list(Json::arrayValue);
    for (const auto& subscriber : subscribers) {
        list.append(subscriber.toJson());
    }
    root["subscribers"] = list;
    Json::Value blocked(Json::arrayValue);
    for (const auto& ip : blockedIps) blocked.append(ip);
    root["blocked_ips"] = blocked;
    return root;
}

SubscriberListConfig SubscriberListConfig::fromJson(const Json::Value& root) {
    SubscriberListConfig config;
    config.filteringEnabled = root.get("filtering_enabled", false).asBool();
    if (root["subscribers"].isArray()) {
        for (const auto& item : root["subscribers"]) {
            config.subscribers.push_back(SubscriberConfig::fromJson(item));
        }
    }
    if (root["blocked_ips"].isArray()) {
        for (const auto& item : root["blocked_ips"]) {
            const std::string ip = normalizeIpAddress(item.asString());
            if (!ip.empty() && std::find(config.blockedIps.begin(), config.blockedIps.end(), ip) == config.blockedIps.end()) {
                config.blockedIps.push_back(ip);
            }
        }
    }
    return config;
}

bool ConfigManager::load() {
    if (!std::filesystem::exists(configPath)) {
        std::cerr << "Config file not found, creating default configuration: " << configPath << std::endl;
        AppConfig defaultConfig;
        {
            std::lock_guard<std::mutex> lock(fileMutex);
            config = defaultConfig;
        }
        if (!save()) return false;
        return loadSubscribers();
    }

    bool migratePlaintextPassword = false;
    {
        std::lock_guard<std::mutex> lock(fileMutex);
        std::ifstream input(configPath);
        if (!input.is_open()) {
            return false;
        }
        Json::Value root;
        Json::CharReaderBuilder readerBuilder;
        std::string errs;
        bool ok = Json::parseFromStream(readerBuilder, input, &root, &errs);
        if (!ok) {
            std::cerr << "Failed to parse config: " << errs << std::endl;
            return false;
        }

        const std::string storedPassword = root.get("password", "admin").asString();
        std::string runtimePassword;
        if (!decryptUiPassword(configPath, storedPassword, runtimePassword)) {
            return false;
        }
        root["password"] = runtimePassword;
        migratePlaintextPassword = storedPassword.rfind(kUiPasswordPrefix, 0) != 0;
        config = AppConfig::fromJson(root);
    }

    // Transparently migrate older plaintext configurations on the first load.
    // Authentication continues to use the plaintext password only in memory.
    if (migratePlaintextPassword) {
        if (!save()) return false;
        std::cerr << "UI password storage migrated to AES-256-GCM; key="
                  << uiPasswordKeyPath(configPath) << " mode=0600" << std::endl;
    }
    return loadSubscribers();
}

bool ConfigManager::loadSubscribers() {
    const auto path = std::filesystem::current_path() / "tvstreammersat5-subscribers.json";
    if (!std::filesystem::exists(path)) {
        subscribers = SubscriberListConfig{};
        return saveSubscribers();
    }
    std::lock_guard<std::mutex> lock(fileMutex);
    std::ifstream input(path);
    if (!input.is_open()) return false;
    Json::Value root;
    Json::CharReaderBuilder readerBuilder;
    std::string errs;
    if (!Json::parseFromStream(readerBuilder, input, &root, &errs)) {
        std::cerr << "Failed to parse subscribers config: " << errs << std::endl;
        return false;
    }
    subscribers = SubscriberListConfig::fromJson(root);
    return true;
}

bool ConfigManager::save() {
    std::lock_guard<std::mutex> lock(fileMutex);

    Json::Value root = config.toJson();
    std::string encryptedPassword;
    if (!encryptUiPassword(configPath, config.password, encryptedPassword)) {
        std::cerr << "Unable to encrypt UI password; configuration was not written" << std::endl;
        return false;
    }
    root["password"] = encryptedPassword;

    std::ofstream output(configPath, std::ios::trunc);
    if (!output.is_open()) {
        std::cerr << "Unable to open config file for writing: " << configPath << std::endl;
        return false;
    }
    Json::StreamWriterBuilder writer;
    writer["indentation"] = "  ";
    output << Json::writeString(writer, root);
    output.flush();
    if (!output.good()) {
        std::cerr << "Unable to write config file: " << configPath << std::endl;
        return false;
    }
    output.close();
    ::chmod(configPath.c_str(), S_IRUSR | S_IWUSR);
    return true;
}

bool ConfigManager::saveSubscribers() {
    std::lock_guard<std::mutex> lock(fileMutex);
    const auto path = std::filesystem::current_path() / "tvstreammersat5-subscribers.json";
    std::ofstream output(path);
    if (!output.is_open()) return false;
    Json::StreamWriterBuilder writer;
    writer["indentation"] = "  ";
    output << Json::writeString(writer, subscribers.toJson());
    return true;
}
