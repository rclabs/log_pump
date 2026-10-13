CXX ?= c++
PYTHON ?= python3
CXXFLAGS ?= -std=c++17 -O2 -Wall -Wextra -Wpedantic
HEADERS := $(wildcard include/log_pump/*.hpp)

.PHONY: all test clean
all: test
build/recorder_tests: tests/recorder_tests.cpp $(HEADERS)
	mkdir -p build
	$(CXX) -Iinclude $(CXXFLAGS) $< $(LDFLAGS) -pthread -o $@

test: build/recorder_tests
	./build/recorder_tests
	$(PYTHON) -m unittest discover -s tests -p 'test_*.py' -v

clean:
	rm -rf build
