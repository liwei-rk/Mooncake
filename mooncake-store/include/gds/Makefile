# Makefile for GDS Mock Library

CXX = g++
CXXFLAGS = -std=c++17 -Wall -Wextra -O2 -fPIC
INCLUDES = -I.

# Target library
TARGET = libndsclient.a 
SHAREDTARGET = libndsclient.so

# Source files
SRCS = gds_mock.cpp
OBJS = $(SRCS:.cpp=.o)

# Test executable
TEST_TARGET = test_gds_mock
TEST_SRCS = test_gds_mock.cpp
TEST_OBJS = $(TEST_SRCS:.cpp=.o)

.PHONY: all clean test install

all: $(TARGET) $(SHAREDTARGET) $(TEST_TARGET)

# Static library
$(TARGET): $(OBJS)
	ar rcs $@ $^

# Shared library
$(SHAREDTARGET): $(OBJS)
	$(CXX) -shared -o $@ $^ $(CXXFLAGS)

# Compile source files
%.o: %.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) -c $< -o $@

# Test executable
$(TEST_TARGET): $(TEST_OBJS) $(TARGET) gds_mock_test_utils.o
	$(CXX) -o $@ $(TEST_OBJS) gds_mock_test_utils.o -L. -lndsclient $(CXXFLAGS)

test: $(TEST_TARGET)
	LD_LIBRARY_PATH=.:$LD_LIBRARY_PATH ./$(TEST_TARGET) 

install: $(TARGET) $(SHAREDTARGET)
	@echo "Installing GDS mock library..."
	@mkdir -p $(DESTDIR)/usr/local/lib
	@mkdir -p $(DESTDIR)/usr/local/include/gds
	cp $(TARGET) $(DESTDIR)/usr/local/lib/
	cp $(SHAREDTARGET) $(DESTDIR)/usr/local/lib/
	cp gds_mock.h gds_interface.h $(DESTDIR)/usr/local/include/gds/
	@echo "Installation complete."

clean:
	rm -f $(OBJS) $(TEST_OBJS) $(TARGET) $(SHAREDTARGET) $(TEST_TARGET)

# Dependencies
gds_mock.o: gds_interface.h
test_gds_mock.o: gds_mock_test_utils.h gds_interface.h
