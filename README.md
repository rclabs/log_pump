# log_pump

A small, domain-independent observation recorder for C++17 / FreeRTOS. Producers
encode fixed-size observations into an owned, bounded queue; an application-owned
consumer task drains it to a destination. No sensor, estimator, SD, Wi-Fi, or
flight-specific definitions live here.

**Initial scope:** observations only, fixed-size numeric payloads, drop-new on
capacity exhaustion, explicit binary encoding, checksummed blocks, YAML code
generation, and a matching host decoder. Event delivery guarantees, diagnostics queues,
transport protocols and visualizers remain application concerns. A bounded
freshness-first consumer policy now provides rate/priority scheduling, replacement,
age expiry and conservative rate adaptation independently of recorder ingress.

## Project boundaries

- `include/log_pump/wire.hpp`: records, explicit little-endian encoders, stream and
  block headers, CRC32. Independent of FreeRTOS.
- `include/log_pump/recorder.hpp`: generic typed producer API and counters.
- `include/log_pump/freertos_queue.hpp`: statically allocated FreeRTOS queue policy.
- `include/log_pump/host_queue.hpp`: bounded mutex-based host/demo queue policy.
- `include/log_pump/freshness_sink.hpp`: fixed latest-value slots, weighted
  high/medium/low scheduling, byte-budget pacing, backlog feedback, a mock link
  provider and nonblocking callback fan-out. Caller serializes access; no I/O or
  domain knowledge. Rates are ceilings, not delivery guarantees. Tokens have a
  256-byte burst bound, so one payload plus its application envelope must fit it.
  Eligible pending wait (not latest sample age) drives adaptation; replacing a
  sample cannot hide a stuck sink, and intentional requested-rate waits are excluded. Priority never authorizes exceeding requested rate.
- `tools/generate.py`: validates a domain-owned YAML dictionary and generates
  `observations.hpp` plus normalized `schema.json`.
- `tools/decode.py`: matching dictionary + stream -> JSON Lines.
- `tests/`: generic format, generator, decoder, ownership and overflow tests.

See the sibling `log_pump_example` repository for domain definitions and a
Makefile-driven runnable example. Generated files belong to the consuming app's
build directory, not this library.

## Build and test

Requires C++17, Python 3, PyYAML 6, and Make (the example uses GNU Make 4.3+).

```sh
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
make test PYTHON=.venv/bin/python
# Or, when PyYAML is already installed:
make test

# Alternative host build:
cmake -S . -B build/cmake
cmake --build build/cmake
ctest --test-dir build/cmake --output-on-failure
```

ESP-IDF consumers can add this checkout as a component; `CMakeLists.txt` exports
headers and requires `freertos`. Static FreeRTOS allocation must be enabled. The
ESP32-S3 adapter is compile-checked against ESP-IDF; device execution and latency
measurements must be performed by the consuming application.

## Generate a domain dictionary

The application owns a schema with `schema_version: 1`, a C++ `namespace`, and a
nonempty `records` list. Records have a unique ID (1..65535), C++ struct `name`,
optional `description`, `category: observation`, and ordered `fields`. Fields
have `name`, `type`, optional `count` (default 1), and optional semantic annotations:
`units`, `frame`, `components`, `description`.

Supported types: `uint8/int8`, `uint16/int16`, `uint32/int32`, `uint64/int64`,
`float32`, `float64`. Count > 1 generates `std::array`; count 1 generates a scalar.
Payloads are fixed-size, at most 65535 bytes. All integers are fixed-width; signed
wire representation is two's complement. Floats require IEEE-754.

```sh
python3 tools/generate.py /path/to/observations.yaml --output-dir /path/to/build/generated
python3 tools/decode.py /path/to/build/generated/schema.json /path/to/recording.lpump
```

The generator produces plain aggregate structs with value-initialized fields,
`type_id`, `encoded_size`, and `encode(uint8_t*)`. It also emits `schema_hash` and
`max_payload_size`. No YAML processing or Python code runs on the MCU. Unsupported
schema keys/types and C++ identifier collisions fail generation. Schema inputs
are trusted developer configuration, not a network input format.

## Producer / consumer integration

```cpp
#include "observations.hpp" // Application-generated definitions
#include "log_pump/freertos_queue.hpp"
#include "log_pump/recorder.hpp"

using Item = log_pump::Record<my_observations::max_payload_size>;
static log_pump::FreeRtosQueue<Item, 128> queue;
static log_pump::Recorder<my_observations::max_payload_size, decltype(queue)> recorder(queue);

// Producer task: populate a generated observation, then:
// bool accepted = recorder.record(observation, capture_time_us);

// Consumer task: blocking wait is permitted here, not on the producer:
// Item item;
// if (queue.receive(item, pdMS_TO_TICKS(100))) { ...encode/write item... }
```

The application starts/stops tasks, selects timestamps, applies capture-rate
policies, and implements sinks. The queue and recorder must outlive all users.
Place substantial queue storage in static/object storage, not on a small task
stack. No public producer method is ISR-safe. Observations must not change while
`record` encodes them. One recorder must use one dictionary; mixing schemas
with overlapping type IDs is an application error.

Producer calls perform bounded encoding and a zero-timeout queue send, with no
heap allocation, text formatting, filesystem/network calls, or waiting for queue
capacity. FreeRTOS uses short internal critical sections: this is not a claim of
zero scheduling interference or measured worst-case execution time. The host
adapter's mutex may wait; it is not a real-time substitute.

FreeRTOS copies the entire fixed-size item into/out of queue storage. No borrowed
pointers are retained. Approximate item storage is `Capacity * sizeof(Item)` plus
queue metadata. Large optional diagnostics should not dictate observation slot
size; separate queues/categories can be added later.

Recording is best effort. Full queues reject the incoming record. `record` returns false and increments
`dropped`; `attempted` and `accepted` are also available. Counters/sequence wrap
modulo 2^32. A sequence is assigned to every attempt, so drops produce gaps.
Concurrent producers can enqueue in a different order from sequence allocation
or measurement time. Counters are individually atomic, not a coherent snapshot
while producers run. Source/sample correlation IDs can be schema payload fields.

Counters report enqueue success, **not sink delivery**. The application must track
SD write failures / Wi-Fi delivery separately. No bounded nonblocking recorder can
guarantee lossless recording if its sink stalls indefinitely.

## Binary format v1

All numeric fields are little-endian. C++ struct layouts are never written.

Stream header (44 bytes):

| Bytes | Meaning |
| --- | --- |
| 8 | `LOGPUMP\0` magic |
| 2 | Format version = 1 |
| 2 | Header length = 44 |
| 32 | SHA-256 of canonical normalized dictionary |

Block header (12 bytes), followed by its record bytes:

| Bytes | Meaning |
| --- | --- |
| 4 | `LPB1` magic |
| 4 | Block body length; 1..1 MiB |
| 4 | IEEE CRC32 of the body (same as Python zlib.crc32) |

Record header (16 bytes), followed by payload:

| Bytes | Meaning |
| --- | --- |
| 2 | Record type ID |
| 2 | Payload length |
| 8 | Caller-supplied monotonic capture timestamp in microseconds |
| 4 | Attempt sequence |

A block contains one or more whole records; records never span blocks. Consumers
encode only actual payload lengths; unused queue-slot bytes are not written.
`Record::encode` and `blockHeader` are low-level helpers: callers must provide valid
lengths and sufficiently large storage. CRC/block assembly belongs on the consumer
side. Batch several records for efficient SD writes. CRC detects accidental
corruption, not malicious modification.

The dictionary hash includes semantic annotations and namespace, with normalized
counts/categories and records sorted by ID. YAML formatting/key ordering does not
change identity; field order and semantic changes do. Archive the exact dictionary
with every log: a hash cannot recover a lost dictionary. Published IDs/layouts/
meanings should remain immutable; changed definitions get new IDs. A changed
schema needs a new stream header/file. Firmware revision, mounting/calibration,
device identity, session identifier, and clock convention should be archived in
application-owned session metadata; they are not embedded automatically.

The decoder rejects wrong dictionaries, corrupted blocks, and malformed lengths.
`--allow-truncated-tail` preserves preceding complete blocks after power loss by
ignoring an incomplete final block. It does not suppress checksum failures or
resynchronize through damaged blocks. Arbitrary fragments are not self-describing
without the matching stream header/dictionary. Unknown IDs are emitted as hex.
Nonfinite float values are preserved as JSON strings `nan`, `+inf`, `-inf`, not
silently replaced with zero. Units/frames remain available in the dictionary.

Wi-Fi packetization, session announcements, receiver feedback and loss handling
are separate sink concerns. SD and Wi-Fi should not share a backlog that forces
both destinations to follow the slower sink's behavior.
