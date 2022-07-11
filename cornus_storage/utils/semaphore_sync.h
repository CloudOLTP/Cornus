#pragma once

#include "global.h"

class SemaphoreSync {
public:
    SemaphoreSync();
    uint32_t          incr();
    uint32_t          decr();
    void              wait();
    void              reset();
private:
    uint32_t          _semaphore;
    pthread_cond_t *  _cond;
    pthread_mutex_t * _mutex;
};

