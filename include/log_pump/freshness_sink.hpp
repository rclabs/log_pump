#pragma once

#include "wire.hpp"
#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>

namespace log_pump {
// Serialized owner API: caller supplies synchronization, time and transport.
// No allocation, locks, threads, sockets, files or domain definitions here.
struct LinkCharacteristics {
    std::uint32_t budget_bytes_per_second = 16000;
    bool simulated = true;
};
class MockLinkCharacteristics {
public:
    explicit MockLinkCharacteristics(std::uint32_t budget = 16000) : budget_(budget) {}
    LinkCharacteristics sample() const { return {budget_, true}; }
private:
    std::uint32_t budget_;
};
enum class Priority : std::uint8_t { Low, Medium, High };
struct Subscription {
    std::uint16_t type = 0;
    std::uint64_t interval_us = 0; // Zero means every new sample, not repeated state.
    std::uint64_t max_age_us = 200000;
    Priority priority = Priority::Medium;
};
struct FreshnessStats {
    std::uint64_t offered = 0, replaced = 0, expired = 0, selected = 0;
    std::uint64_t completed = 0, failed = 0, completed_bytes = 0;
};
template<std::size_t MaxPayload, std::size_t Streams>
class FreshnessSink {
public:
    using Item = Record<MaxPayload>;
    struct Slot {
        Subscription subscription{};
        FreshnessStats stats{};
        Item latest{};
        bool pending = false, sent_once = false;
        std::uint64_t pending_since_us = 0, last_selected_us = 0;
        unsigned credits = 0;
    };
    // Atomic replacement is the wrapper's responsibility. Validate first.
    bool configure(const Subscription* subscriptions, std::size_t count, std::uint64_t now_us) {
        if (count > Streams || (count != 0 && subscriptions == nullptr)) return false;
        for (std::size_t i = 0; i < count; ++i) {
            if (subscriptions[i].type == 0 || subscriptions[i].max_age_us == 0 || subscriptions[i].interval_us > (UINT64_MAX >> 3) ||
                static_cast<unsigned>(subscriptions[i].priority) > 2) return false;
            for (std::size_t j = 0; j < i; ++j)
                if (subscriptions[i].type == subscriptions[j].type) return false;
        }
        slots_ = {};
        count_ = count;
        for (std::size_t i = 0; i < count; ++i) slots_[i].subscription = subscriptions[i];
        level_ = 0;
        cursor_ = 0;
        tokens_ = 256;
        token_us_ = feedback_us_ = last_pressure_us_ = now_us;
        return true;
    }
    bool offer(const Item& record, std::uint64_t now_us) {
        if (record.payload_size > MaxPayload) return false;
        for (std::size_t i = 0; i < count_; ++i) {
            auto& slot = slots_[i];
            if (record.type != slot.subscription.type) continue;
            ++slot.stats.offered;
            if (slot.pending) ++slot.stats.replaced;
            else slot.pending_since_us = now_us;
            slot.latest = record; // Copy owned; source may immediately be reused.
            slot.pending = true;
            return true;
        }
        return false;
    }
    std::size_t pending() const {
        std::size_t result = 0;
        for (std::size_t i = 0; i < count_; ++i) result += slots_[i].pending ? 1U : 0U;
        return result;
    }
    std::size_t pendingBytes() const {
        std::size_t result = 0;
        for (std::size_t i = 0; i < count_; ++i)
            if (slots_[i].pending) result += slots_[i].latest.payload_size;
        return result;
    }
    std::uint64_t oldestPendingAge(std::uint64_t now_us) const {
        std::uint64_t result = 0;
        for (std::size_t i = 0; i < count_; ++i)
            if (slots_[i].pending && now_us >= slots_[i].pending_since_us)
                result = std::max(result, now_us - slots_[i].pending_since_us);
        return result;
    }
    void feedback(std::uint64_t now_us, bool transport_busy) {
        if (now_us < feedback_us_ || now_us - feedback_us_ < 100000) return;
        feedback_us_ = now_us;
        if (transport_busy || oldestPendingAge(now_us) >= 100000) {
            level_ = std::min(3U, level_ + 1);
            last_pressure_us_ = now_us;
        } else if (level_ != 0 && now_us >= last_pressure_us_ && now_us - last_pressure_us_ >= 2000000) {
            --level_;
            last_pressure_us_ = now_us;
        }
    }
    std::uint64_t effectiveInterval(std::size_t i) const {
        const auto& subscription = slots_[i].subscription;
        // Even max streams acquire a conservative rate ceiling under pressure.
        const auto base = subscription.interval_us != 0 ? subscription.interval_us : (level_ != 0 ? 10000U : 0U);
        const auto exponent = subscription.priority == Priority::High ? level_ / 3 :
                              subscription.priority == Priority::Medium ? level_ / 2 : level_;
        return base << exponent;
    }
    bool take(Item& result, std::size_t& stream_index, std::uint64_t now_us,
              LinkCharacteristics link, std::size_t envelope_bytes) {
        if (now_us < token_us_) token_us_ = now_us;
        const auto elapsed = std::min<std::uint64_t>(now_us - token_us_, 1000000);
        tokens_ = std::min(256.0, tokens_ + static_cast<double>(elapsed) * link.budget_bytes_per_second / 1e6);
        token_us_ = now_us;
        for (std::size_t i = 0; i < count_; ++i) {
            auto& slot = slots_[i];
            if (slot.pending && (now_us < slot.latest.timestamp_us ||
                now_us - slot.latest.timestamp_us >= slot.subscription.max_age_us)) {
                slot.pending = false;
                ++slot.stats.expired;
            }
        }
        // Weighted round robin: high/medium/low get 4/2/1 opportunities per round.
        // At most two passes; an idle/ineligible stream cannot stall the others.
        for (unsigned pass = 0; pass < 2; ++pass) {
            for (std::size_t n = 0; n < count_; ++n) {
                const auto i = (cursor_ + n) % count_;
                auto& slot = slots_[i];
                const auto interval = effectiveInterval(i);
                if (!slot.pending || slot.credits == 0 ||
                    (slot.sent_once && now_us >= slot.last_selected_us && now_us - slot.last_selected_us < interval)) continue;
                const auto bytes = envelope_bytes + slot.latest.payload_size;
                if (tokens_ < static_cast<double>(bytes)) continue;
                tokens_ -= static_cast<double>(bytes);
                result = slot.latest;
                stream_index = i;
                slot.pending = false;
                slot.sent_once = true;
                slot.last_selected_us = now_us;
                --slot.credits;
                ++slot.stats.selected;
                cursor_ = slot.credits == 0 ? (i + 1) % count_ : i;
                return true;
            }
            for (std::size_t i = 0; i < count_; ++i)
                slots_[i].credits = slots_[i].subscription.priority == Priority::High ? 4U :
                                    slots_[i].subscription.priority == Priority::Medium ? 2U : 1U;
        }
        return false;
    }
    void complete(std::size_t index, bool success, std::size_t bytes) {
        if (index >= count_) return;
        auto& stats = slots_[index].stats;
        if (success) { ++stats.completed; stats.completed_bytes += bytes; }
        else ++stats.failed; // Never retry an uncertain send.
    }
    const Slot& slot(std::size_t index) const { return slots_[index]; }
    std::size_t size() const { return count_; }
    unsigned adaptationLevel() const { return level_; }
private:
    std::array<Slot, Streams> slots_{};
    std::size_t count_ = 0, cursor_ = 0;
    unsigned level_ = 0;
    double tokens_ = 256;
    std::uint64_t token_us_ = 0, feedback_us_ = 0, last_pressure_us_ = 0;
};

// Nonblocking fan-out callbacks must copy before returning and must never do I/O.
// Each independent sink receives the same record; failures do not gate later sinks.
template<class Item, std::size_t Sinks> class Fanout {
public:
    using Offer = bool (*)(void*, const Item&);
    bool add(Offer callback, void* context) {
        if (!callback || count_ == Sinks) return false;
        entries_[count_++] = {callback, context};
        return true;
    }
    std::size_t offer(const Item& item) const {
        std::size_t accepted = 0;
        for (std::size_t i = 0; i < count_; ++i)
            accepted += entries_[i].offer(entries_[i].context, item) ? 1U : 0U;
        return accepted;
    }
private:
    struct Entry { Offer offer = nullptr; void* context = nullptr; };
    std::array<Entry, Sinks> entries_{};
    std::size_t count_ = 0;
};
} // namespace log_pump
