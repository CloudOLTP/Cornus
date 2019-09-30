#pragma once

#include "global.h"

class workload;
class QueryBase;
class Manager;

class Thread {
public:
    enum ThreadType {WORKER_THREAD,
                      INPUT_THREAD,
                     OUTPUT_THREAD};

    Thread(uint64_t thd_id, ThreadType thread_type);
    uint64_t         get_thd_id()        { return _thd_id; };
    ThreadType         get_thd_type()        { return _thread_type; };

    virtual RC         run() = 0;
protected:
    workload *         _wl;
    uint64_t         _thd_id;
    ThreadType         _thread_type;
};
