#include "log_pump/host_queue.hpp"
#include "log_pump/recorder.hpp"
#include <cstdlib>
#include <iostream>
#include <thread>
#define CHECK(condition) do { if (!(condition)) { std::cerr << __LINE__ << ": " << #condition << '\n'; std::exit(1); } } while (0)

struct TestObservation {
    static constexpr std::uint16_t type_id = 7;
    static constexpr std::size_t encoded_size = 4;
    std::uint32_t value;
    void encode(std::uint8_t* out) const { log_pump::putUnsigned(out, value, 4); }
};
int main() {
    using Item = log_pump::Record<4>;
    log_pump::HostQueue<Item, 2> queue;
    log_pump::Recorder<4, decltype(queue)> recorder(queue);
    TestObservation source{0x12345678};
    CHECK(recorder.tryRecord(source, 0x0102030405060708ULL));
    source.value = 0;
    CHECK(recorder.tryRecord(source, 10));
    CHECK(!recorder.tryRecord(source, 20));
    Item record;
    CHECK(recorder.tryReceive(record));
    CHECK(record.sequence == 0 && record.timestamp_us == 0x0102030405060708ULL);
    CHECK(record.payload[0] == 0x78 && record.payload[3] == 0x12);
    std::array<std::uint8_t, 20> encoded{};
    CHECK(record.encode(encoded.data()) == 20);
    const std::array<std::uint8_t, 20> expected{{7,0,4,0,8,7,6,5,4,3,2,1,0,0,0,0,0x78,0x56,0x34,0x12}};
    CHECK(encoded == expected);
    CHECK(recorder.tryRecord(source, 30)); // Wrap queue after a drop.
    CHECK(recorder.tryReceive(record) && record.sequence == 1);
    CHECK(recorder.tryReceive(record) && record.sequence == 3);
    CHECK(!recorder.tryReceive(record));
    auto stats = recorder.stats();
    CHECK(stats.attempted == 4 && stats.accepted == 3 && stats.dropped == 1);
    const auto* text = reinterpret_cast<const std::uint8_t*>("123456789");
    CHECK(log_pump::crc32(text, 9) == 0xcbf43926u);
    CHECK(log_pump::crc32(text, 0) == 0);
    // Exercise atomic accounting with concurrent producers and full capacity.
    auto producer = [&] { for (int i = 0; i < 1000; ++i) recorder.tryRecord(source, i); };
    std::thread first(producer), second(producer);
    first.join(); second.join();
    stats = recorder.stats();
    CHECK(stats.attempted == 2004 && stats.accepted == 5 && stats.dropped == 1999);
    std::cout << "Queue ownership, overflow, encoding, CRC and concurrent accounting passed\n";
}
