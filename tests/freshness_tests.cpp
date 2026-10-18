#include "log_pump/freshness_sink.hpp"
#include <cassert>
#include <cstdio>

using Sink = log_pump::FreshnessSink<8, 3>;
using Item = Sink::Item;
int main() {
    Sink sink;
    log_pump::Subscription subscriptions[] = {{1, 1000000, 2000000, log_pump::Priority::Low},
                                               {2, 0, 2000000, log_pump::Priority::High}};
    assert(sink.configure(subscriptions, 2, 0));
    Item first; first.type = 1; first.payload_size = 1; first.payload[0] = 42;
    assert(sink.offer(first, 0));
    first.payload[0] = 43;
    assert(sink.offer(first, 1));
    assert(sink.pending() == 1 && sink.slot(0).stats.replaced == 1);
    Item out; std::size_t index = 99;
    assert(sink.take(out, index, 1, {}, 16));
    assert(out.payload[0] == 43 && index == 0);
    sink.complete(index, true, 17);
    assert(sink.slot(0).stats.completed == 1);
    first.timestamp_us = 100;
    assert(sink.offer(first, 100));
    assert(!sink.take(out, index, 999999, {}, 16));
    sink.feedback(999999, false);
    assert(sink.adaptationLevel() == 0); // Requested rate wait is not congestion.
    assert(sink.take(out, index, 1000001, {}, 16));
    assert(!sink.take(out, index, 1000001, {}, 16)); // No repeated value.
    assert(sink.offer(first, 1000002));
    assert(!sink.take(out, index, 3000000, {}, 16)); // Stale samples expire.
    assert(sink.slot(0).stats.expired == 1);
    sink.feedback(3000000, true);
    assert(sink.adaptationLevel() == 1);
    assert(sink.effectiveInterval(0) == 2000000);
    sink.feedback(5100000, false);
    assert(sink.adaptationLevel() == 0);
    auto invalid = subscriptions[0]; invalid.type = 0;
    assert(!sink.configure(&invalid, 1, 0) && sink.size() == 2);
    log_pump::Subscription duplicate[] = {subscriptions[0], subscriptions[0]};
    assert(!sink.configure(duplicate, 2, 0));
    first.payload_size = 9;
    assert(!sink.offer(first, 0));
    assert(sink.configure(nullptr, 0, 0));
    assert(!sink.take(out, index, 0, {}, 16));
    // Both subscribers see a copy, and a rejecting sink cannot gate the next.
    log_pump::Fanout<Item, 2> fanout;
    unsigned calls = 0;
    assert(fanout.add([](void* p, const Item&) { ++*static_cast<unsigned*>(p); return false; }, &calls));
    assert(fanout.add([](void* p, const Item&) { ++*static_cast<unsigned*>(p); return true; }, &calls));
    assert(!fanout.add(nullptr, nullptr));
    assert(fanout.offer(out) == 1 && calls == 2);
    // Sustained saturation serves all classes, with preferential high service.
    log_pump::Subscription weighted[] = {{1, 0, 2000000, log_pump::Priority::Low},
                                        {2, 0, 2000000, log_pump::Priority::Medium},
                                        {3, 0, 2000000, log_pump::Priority::High}};
    assert(sink.configure(weighted, 3, 0));
    unsigned counts[3] = {};
    for (unsigned tick = 1; tick <= 700; ++tick) {
        for (unsigned id = 1; id <= 3; ++id) {
            first.type = static_cast<std::uint16_t>(id); first.payload_size = 1;
            first.timestamp_us = tick * 1000ULL;
            sink.offer(first, first.timestamp_us);
        }
        if (sink.take(out, index, tick * 1000ULL, {100000, true}, 16)) ++counts[index];
    }
    assert(counts[2] > counts[1] && counts[1] > counts[0] && counts[0] > 0);
    std::puts("Freshness ownership, rate, age, fairness, adaptation and independent fanout passed");
}
