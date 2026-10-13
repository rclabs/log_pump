#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>

namespace log_pump {
// All wire values are little-endian. Never serialize C++ object layouts.
inline void putUnsigned(std::uint8_t* out, std::uint64_t value, std::size_t bytes) {
    for (std::size_t i = 0; i < bytes; ++i) out[i] = static_cast<std::uint8_t>(value >> (8 * i));
}
inline void putFloat32(std::uint8_t* out, float value) {
    static_assert(sizeof(float) == 4 && std::numeric_limits<float>::is_iec559, "IEEE-754 float32 required");
    std::uint32_t bits;
    std::memcpy(&bits, &value, sizeof(bits));
    putUnsigned(out, bits, 4);
}
inline void putFloat64(std::uint8_t* out, double value) {
    static_assert(sizeof(double) == 8 && std::numeric_limits<double>::is_iec559, "IEEE-754 float64 required");
    std::uint64_t bits;
    std::memcpy(&bits, &value, sizeof(bits));
    putUnsigned(out, bits, 8);
}
constexpr std::size_t stream_header_size = 44;
constexpr std::size_t record_header_size = 16;
constexpr std::size_t block_header_size = 12;
constexpr std::size_t max_block_size = 1024 * 1024;
using SchemaHash = std::array<std::uint8_t, 32>;

inline std::array<std::uint8_t, stream_header_size> streamHeader(const SchemaHash& schema_hash) {
    std::array<std::uint8_t, stream_header_size> out{};
    const char magic[8] = {'L', 'O', 'G', 'P', 'U', 'M', 'P', '\0'};
    std::memcpy(out.data(), magic, 8);
    putUnsigned(out.data() + 8, 1, 2); // format version
    putUnsigned(out.data() + 10, stream_header_size, 2);
    std::memcpy(out.data() + 12, schema_hash.data(), schema_hash.size());
    return out;
}
inline std::uint32_t crc32(const std::uint8_t* bytes, std::size_t size) {
    std::uint32_t crc = 0xffffffffu;
    for (std::size_t i = 0; i < size; ++i) {
        crc ^= bytes[i];
        for (int bit = 0; bit < 8; ++bit)
            crc = (crc >> 1) ^ ((crc & 1) ? 0xedb88320u : 0u);
    }
    return crc ^ 0xffffffffu;
}
inline std::array<std::uint8_t, block_header_size> blockHeader(const std::uint8_t* records, std::size_t size) {
    // Caller must supply a nonempty block no larger than max_block_size.
    std::array<std::uint8_t, block_header_size> out{};
    std::memcpy(out.data(), "LPB1", 4);
    putUnsigned(out.data() + 4, size, 4);
    putUnsigned(out.data() + 8, crc32(records, size), 4);
    return out;
}

template<std::size_t MaxPayload>
struct Record {
    static_assert(MaxPayload <= 65535, "Payload length is uint16 on wire");
    std::uint64_t timestamp_us = 0; // Caller-defined monotonic capture time.
    std::uint32_t sequence = 0;     // Attempt sequence, wraps modulo 2^32.
    std::uint16_t type = 0;
    std::uint16_t payload_size = 0;
    std::array<std::uint8_t, MaxPayload> payload{};

    // Consumer-only serialization; output must hold record_header_size + MaxPayload.
    std::size_t encode(std::uint8_t* out) const {
        putUnsigned(out, type, 2);
        putUnsigned(out + 2, payload_size, 2);
        putUnsigned(out + 4, timestamp_us, 8);
        putUnsigned(out + 12, sequence, 4);
        std::memcpy(out + record_header_size, payload.data(), payload_size);
        return record_header_size + payload_size;
    }
};
} // namespace log_pump
