CC=g++
PROTOC=protoc
CPPF = `pkg-config --cflags protobuf grpc`

.SUFFIXES: .o .cpp .h 

SRC_DIRS = ./ ./benchmarks/ ./concurrency_control/ ./storage/ ./system/ ./transport/ ./proto/ ./utils/
GRPC_CPP_PLUGIN = grpc_cpp_plugin
GRPC_CPP_PLUGIN_PATH ?= `which $(GRPC_CPP_PLUGIN)`

CFLAGS=-Wall -g -std=c++11
INCLUDE = -I. -I./benchmarks -I./concurrency_control -I./storage -I./system -I./transport -I./proto -I./utils
CFLAGS += $(INCLUDE) -D NOGRAPHITE=1 -Werror -O3

LDFLAGS = -Wall -L. -L./libs -pthread -g -lrt -std=c++11 -O3 -ljemalloc
LDFLAGS += -L/usr/local/lib `pkg-config --libs protobuf grpc++`\
           -Wl,--no-as-needed -lgrpc++_reflection -Wl,--as-needed\
           -ldl

CPPS = $(foreach dir, $(SRC_DIRS), $(wildcard $(dir)*.cpp))
OBJS = $(CPPS:.cpp=.o)
DEPS = $(CPPS:.cpp=.d)

#vpath %.proto $(PROTOS_PATH)

all : rundb

rundb : $(OBJS) 
	$(CC) -no-pie -o $@ $^ $(LDFLAGS)

-include $(OBJS:%.o=%.d)

%.d: %.cpp
	$(CC) -MM -MT $*.o -MF $@ $(CFLAGS) $<

%.o: %.cpp 
	$(CC) $(CPPF) -c $(CFLAGS) -o $@ $<

.PHONY: clean
clean:
	rm -f rundb $(OBJS) $(DEPS) 
