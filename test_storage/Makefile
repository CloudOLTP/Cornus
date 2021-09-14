CC=g++
PROTOC=protoc
CPPF = `pkg-config --cflags protobuf grpc`

.SUFFIXES: .o .cpp .h 

SRC_DIRS = ./  ../proto/
GRPC_CPP_PLUGIN = grpc_cpp_plugin
GRPC_CPP_PLUGIN_PATH ?= `which $(GRPC_CPP_PLUGIN)`

CFLAGS=-Wall -g -std=c++11
INCLUDE = -I. -I../proto
INCLUDE += -I/home/kanwu/vcpkg/installed/x64-linux/include
CFLAGS += $(INCLUDE) -D NOGRAPHITE=1 -Og -g -fpermissive

LDFLAGS = -Wall -L. -L../libs -pthread -g -lrt -std=c++11 -Og -ljemalloc
#LDFLAGS += -L/usr/local/lib `pkg-config --libs protobuf grpc++`\
#           -Wl,--no-as-needed -lgrpc++_reflection -Wl,--as-needed -ldl\
#           -lcpp_redis -ltacopie
# last line is for redis
# for azure storage
LDFLAGS += -L/home/kanwu/vcpkg/installed/x64-linux/lib -lazurestorage\
           -lcpprest -lboost_system -lboost_log -lboost_log_setup -lboost_thread -lboost_serialization\
           -luuid -lxml2 -lz -llzma -lssl -lcrypto -lpthread -ldl 

LDFLAGS += -L/usr/local/lib `pkg-config --libs protobuf grpc++`\
           -Wl,--no-as-needed -lgrpc++_reflection -Wl,--as-needed -ldl\
           -lcpp_redis -ltacopie
#-I /home/kanwu/vcpkg/installed/x64-linux/include 

CPPS = $(foreach dir, $(SRC_DIRS), $(wildcard $(dir)*.cpp))
OBJS = $(CPPS:.cpp=.o)
DEPS = $(CPPS:.cpp=.d)

#vpath %.proto $(PROTOS_PATH)

all : run_test_network

run_test_network : $(OBJS)
	$(CC) -no-pie -o $@ $^ $(LDFLAGS)

-include $(OBJS:%.o=%.d)

%.d: %.cpp
	$(CC) -MM -MT $*.o -MF $@ $(CFLAGS) $<

%.o: %.cpp 
	$(CC) $(CPPF) -c $(CFLAGS) -o $@ $<

.PHONY: clean
clean:
	rm -f rundb $(OBJS) $(DEPS) 
