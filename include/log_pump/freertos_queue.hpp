#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <type_traits>
#include "freertos/FreeRTOS.h"
#include "freertos/queue.h"

#if configSUPPORT_STATIC_ALLOCATION != 1
#error "log_pump requires FreeRTOS static allocation support"
#endif

namespace log_pump {
// Static bounded storage. FreeRTOS copies sizeof(Item) bytes on send/receive.
// Construct before starting producers; stop all users before destruction.
// No waiting for capacity/data; FreeRTOS still uses short internal critical sections.
template<class Item, std::size_t Capacity>
class FreeRtosQueue {
public:
    static_assert(Capacity > 0, "Queue capacity must be nonzero");
    static_assert(std::is_trivially_copyable<Item>::value, "Queue item must own trivially copyable data");
    FreeRtosQueue() : handle_(xQueueCreateStatic(Capacity, sizeof(Item), storage_.data(), &control_)) {
        configASSERT(handle_ != nullptr);
    }
    ~FreeRtosQueue() { vQueueDelete(handle_); }
    FreeRtosQueue(const FreeRtosQueue&) = delete;
    FreeRtosQueue& operator=(const FreeRtosQueue&) = delete;
    bool tryPush(const Item& item) { return xQueueSend(handle_, &item, 0) == pdPASS; }
    bool tryPop(Item& item) { return xQueueReceive(handle_, &item, 0) == pdPASS; }
    bool receive(Item& item, TickType_t wait) { return xQueueReceive(handle_, &item, wait) == pdPASS; }
    UBaseType_t size() const { return uxQueueMessagesWaiting(handle_); }
private:
    StaticQueue_t control_{};
    alignas(Item) std::array<std::uint8_t, Capacity * sizeof(Item)> storage_{};
    QueueHandle_t handle_;
};
} // namespace log_pump
