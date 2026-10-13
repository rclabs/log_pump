#pragma once

#include "log_pump/wire.hpp"
#include <atomic>
#include <type_traits>

namespace log_pump {
struct RecorderStats {
    std::uint32_t attempted;
    std::uint32_t accepted;
    std::uint32_t dropped;
};

// Queue policy must own copies and provide thread-safe, nonblocking tryPush /
// tryPop. Queue capacity is fixed before producers start. Task context only.
template<std::size_t MaxPayload, class Queue>
class Recorder {
public:
    using RecordType = Record<MaxPayload>;
    explicit Recorder(Queue& queue) : queue_(queue) {
        static_assert(std::atomic<std::uint32_t>::is_always_lock_free,
                      "Recorder counters require lock-free uint32 atomics");
    }
    Recorder(const Recorder&) = delete;
    Recorder& operator=(const Recorder&) = delete;

    template<class Observation>
    bool tryRecord(const Observation& value, std::uint64_t timestamp_us) {
        static_assert(Observation::encoded_size <= MaxPayload, "Observation exceeds queue slot payload");
        RecordType record{};
        record.timestamp_us = timestamp_us;
        record.sequence = attempted_.fetch_add(1, std::memory_order_relaxed);
        record.type = Observation::type_id;
        record.payload_size = Observation::encoded_size;
        value.encode(record.payload.data());
        if (!queue_.tryPush(record)) {
            dropped_.fetch_add(1, std::memory_order_relaxed);
            return false;
        }
        accepted_.fetch_add(1, std::memory_order_relaxed);
        return true;
    }
    bool tryReceive(RecordType& record) { return queue_.tryPop(record); }
    // Individually atomic counters, not a transactional snapshot during recording.
    // Counts wrap modulo 2^32. Call once producers stop for consistent totals.
    RecorderStats stats() const {
        return {attempted_.load(std::memory_order_relaxed), accepted_.load(std::memory_order_relaxed),
                dropped_.load(std::memory_order_relaxed)};
    }
private:
    Queue& queue_;
    std::atomic<std::uint32_t> attempted_{0}, accepted_{0}, dropped_{0};
};
} // namespace log_pump
