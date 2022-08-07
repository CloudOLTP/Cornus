#include "log.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

LogManager::LogManager()
{

}

LogManager::LogManager(const char * log_name)
{
    _buffer_size = 64 * 1024 * 1024;
    int align = 512 - 1;
    _lsn = 0;
    _name_size = 50;
    _log_name = new char[_name_size];
    strcpy(_log_name, log_name);
    //TODO: delete O_TRUNC when recovery is needed.
    _log_fd = open(log_name, O_RDWR | O_CREAT | O_TRUNC | O_APPEND | O_DIRECT, 0755);
    if (_log_fd == 0) {
        perror("open log file");
        exit(1);
    }

    _buffer = (char *)malloc(_buffer_size + align); // 64 MB
    _buffer = (char *)(((uintptr_t)_buffer + align)&~((uintptr_t)align));
    flush_buffer_ = (char *)malloc(_buffer_size + align); // 64 MB
    flush_buffer_ = (char *)(((uintptr_t)flush_buffer_ + align)&~((uintptr_t)align));

    // group commit
    latch_ = new std::mutex();
    flush_latch_ = new std::mutex();
    swap_lock = new std::mutex();
    cv_ = new std::condition_variable();
    appendCv_ = new std::condition_variable();
    flush_cv_ = new std::condition_variable();
    //latch_return_response_ = new std::mutex();

}

LogManager::~LogManager() {
    delete[] _log_name;
    _log_name = nullptr;
    close(_log_fd);
}

void
LogManager::test() {
    printf("test\n");
}


RC LogManager::log(const SundialRequest* request, SundialResponse* reply) {
    #if DEBUG_LOG
reply->set_response_type(request_to_response(request->request_type()));
    #else
    // TODO: add logic for insert once
    log_request(request, reply);
    
    // sleep until flush finish and wakeup
    // CAUTION: race condition may occur but it is rare because flushing is too long
    std::unique_lock<std::mutex> latch(*flush_latch_);
    flush_cv_->wait(latch);
    #endif
    return RCOK;
}

void LogManager::log_request(const SundialRequest* request, SundialResponse * reply) {
    std::unique_lock<std::mutex> latch(*latch_);
    ATOM_FETCH_ADD(_lsn, 1);
    uint32_t size_total = sizeof(LogRecord) + request->log_data_size();
    swap_lock->lock();
    if (logBufferOffset_ + size_total > _buffer_size) {
        needFlush_ = true;
        cv_->notify_one(); //let RunFlushThread wake up.
        appendCv_->wait(latch, [&] {return logBufferOffset_ <= _buffer_size;});
        // logBufferOffset_ = 0;
    }
    LogRecord log{request->txn_id(),
                  _lsn, request_to_log(request->request_type())};
    // format: | node_id | txn_id | lsn | type | size of data(if any) | data(if any)
    memcpy(_buffer + logBufferOffset_, &log, sizeof(log));
    logBufferOffset_ += sizeof(LogRecord);
    if (request->log_data_size() != 0) {
        uint64_t data_size = request->log_data_size();
        memcpy(_buffer + logBufferOffset_, &data_size, sizeof(uint64_t));
        logBufferOffset_ += sizeof(uint64_t);
        memcpy(_buffer + logBufferOffset_, request->log_data().c_str(), data_size);
        logBufferOffset_ += data_size;
    }
    swap_lock->unlock();
    reply->set_response_type(request_to_response(request->request_type()));
}

uint64_t LogManager::get_last_lsn() {
    return _lsn;
}

SundialResponse::ResponseType LogManager::request_to_response(SundialRequest::RequestType type) {
    switch (type)
    {
        case SundialRequest:: LOG_YES_REQ :
            return SundialResponse:: RESP_LOG_YES;
        case SundialRequest:: LOG_COMMIT_REQ :
            return SundialResponse:: RESP_LOG_COMMIT;
        case SundialRequest:: LOG_ABORT_REQ :
            return SundialResponse:: RESP_LOG_ABORT;
        default:
            assert(false);
    }
};

LogRecord::Type LogManager::request_to_log(SundialRequest::RequestType vote) {
    switch (vote)
    {
        case SundialRequest:: LOG_COMMIT_REQ:
            return LogRecord::COMMIT;
        case SundialRequest:: LOG_ABORT_REQ:
            return LogRecord::ABORT;
        case SundialRequest:: LOG_YES_REQ:
            return LogRecord::YES;
        default:
            assert(false);
    }
}
//group commit
// spawn a separate thread to wake up periodically to flush
void LogManager::run_flush_thread() {
    if (ENABLE_LOGGING) return;
    ENABLE_LOGGING = true;
    flush_thread_ = new std::thread([&] {
        while (ENABLE_LOGGING) { //The thread is triggered every LOG_TIMEOUT seconds or when the log buffer is full
            std::unique_lock<std::mutex> latch(*latch_);
            // (2) When LOG_TIMEOUT is triggered.
            cv_->wait_for(latch, log_timeout, [&] {return needFlush_;});
            assert(flushBufferSize_ == 0);
            if (logBufferOffset_ > 0) {

                swap_lock->lock();
                std::swap(_buffer,flush_buffer_);
                std::swap(logBufferOffset_,flushBufferSize_);
                swap_lock->unlock();

                if (write(_log_fd, flush_buffer_, PGROUNDUP(flushBufferSize_)) == -1) {
                    perror("write2");
                    exit(1);
                }
                // }
                if (fsync(_log_fd) == -1) {
                    perror("fsync");
                    exit(1);
                }

                flushBufferSize_ = 0;
                // TODO: finish flushing and notify all
                flush_cv_->notify_all();
            }
            needFlush_ = false;
            appendCv_->notify_all();
        }
    });
};
/*
 * Stop and join the flush thread, set ENABLE_LOGGING = false
 */
void LogManager::stop_flush_thread() {
    if (!ENABLE_LOGGING) return;
    ENABLE_LOGGING = false;
    flush(true);
    flush_thread_->join();
    assert(logBufferOffset_ == 0 && flushBufferSize_ == 0);
    delete flush_thread_;
};

void LogManager::flush(bool force) {
    std::unique_lock<std::mutex> latch(*latch_);
    if (force) {
        needFlush_ = true;
        cv_->notify_one(); //let RunFlushThread wake up.
        if (ENABLE_LOGGING)
            appendCv_->wait(latch, [&] { return !needFlush_; }); //block append thread
    } else {
        appendCv_->wait(latch);// group commit,  But instead of forcing flush,
        // you need to wait for LOG_TIMEOUT or other operations to implicitly trigger the flush operations
    }
}