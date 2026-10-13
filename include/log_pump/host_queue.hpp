#pragma once

#include <array>
#include <cstddef>
#include <mutex>

namespace log_pump {
// Host/demo policy only: mutex acquisition may wait. This is not the embedded
// real-time implementation. Storage is bounded; full queues reject incoming data.
template<class Item, std::size_t Capacity>
class HostQueue {
public:
    static_assert(Capacity > 0, "Queue capacity must be nonzero");
    bool tryPush(const Item& item) {
        std::lock_guard<std::mutex> guard(mutex_);
        if (size_ == Capacity) return false;
        items_[(head_ + size_) % Capacity] = item;
        ++size_;
        return true;
    }
    bool tryPop(Item& item) {
        std::lock_guard<std::mutex> guard(mutex_);
        if (size_ == 0) return false;
        item = items_[head_];
        head_ = (head_ + 1) % Capacity;
        --size_;
        return true;
    }
private:
    std::mutex mutex_;
    std::array<Item, Capacity> items_{};
    std::size_t head_ = 0, size_ = 0;
};
} // namespace log_pump
