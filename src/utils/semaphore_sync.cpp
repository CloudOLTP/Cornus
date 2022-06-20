#include "semaphore_sync.h"

SemaphoreSync::SemaphoreSync()
{
    _cond = new pthread_cond_t;
    _mutex = new pthread_mutex_t;
    pthread_cond_init(_cond, NULL);
    pthread_mutex_init(_mutex, NULL);

    _semaphore = 0;
}

uint32_t
SemaphoreSync::incr()
{
    pthread_mutex_lock(_mutex);
    uint32_t sem = ++_semaphore;
    pthread_mutex_unlock(_mutex);
    return sem;
}

uint32_t
SemaphoreSync::decr() {
    pthread_mutex_lock(_mutex);
    assert(_semaphore != 0);
    uint32_t sem = --_semaphore;
    pthread_mutex_unlock(_mutex);
    if (sem == 0)
        pthread_cond_signal(_cond);
    return sem;
}

void
SemaphoreSync::wait() {
    pthread_mutex_lock(_mutex);
    while (_semaphore > 0)
        pthread_cond_wait(_cond, _mutex);
    pthread_mutex_unlock(_mutex);
}

