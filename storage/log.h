#pragma once
#include "global.h"
#include "helper.h"
#include <map>
#include "log_record.h"


#include "sundial.pb.h"
#include "sundial.grpc.pb.h"

//group commit
#include <condition_variable>
#include <future>
#include <mutex>
#include <atomic>
#include <chrono>
#include <fcntl.h>
#include <unistd.h>

// logging helper
#define PGSIZE 512
#define PGROUNDUP(sz)  (((sz)+PGSIZE-1) & ~(PGSIZE-1))

using sundial_rpc::SundialRequest;
using sundial_rpc::SundialResponse;

typedef struct _chunck_types {
    int yes;
    int commmit;
    int abort;
    uint64_t size;
    uint64_t flushing_time;
} chunck_types;

class LogManager {
public:
    LogManager();
    ~LogManager();
    LogManager(const char *log_name);
    // TODO: replace by sundial grpc
    RC log(const SundialRequest* request, SundialResponse* reply);

    void run_flush_thread();
    void stop_flush_thread();
    void flush(bool force);
    void test();

private:
    uint32_t _buffer_size;
    char * _buffer;
    uint32_t _lsn;
    FILE * _log_fp;
    int _log_fd;
    uint32_t _name_size;
    char * _log_name;

    //group commit
    char *flush_buffer_;
    uint32_t logBufferOffset_ = 0;
    uint32_t flushBufferSize_ = 0;
    bool ENABLE_LOGGING = false;
    std::chrono::duration<long long int, std::micro> log_timeout =
            std::chrono::microseconds(LOG_TIMEOUT);
    bool needFlush_ = false; //for group commit
    std::condition_variable * appendCv_; // for notifying append thread
    // latch for cv
    std::mutex * latch_;
    // flush thread
    std::thread *flush_thread_;
    // for notifying flush thread
    std::condition_variable * cv_;
    // for ensuring log on disk
    std::condition_variable * flush_cv_;

    // LogRecord::Type check_log(Message * msg);
    void log_request(const SundialRequest* request, SundialResponse* reply);
    uint64_t get_last_lsn();
    SundialRequest::RequestType log_to_request(LogRecord::Type vote);
    LogRecord::Type request_to_log(SundialRequest::RequestType vote);
    SundialResponse::ResponseType request_to_response(SundialRequest::RequestType type);

    std::mutex * swap_lock;
    std::vector<chunck_types> debug_chunck;
};