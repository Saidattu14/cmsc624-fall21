#include "thread_pool_launcher.h"

#include <stdlib.h>
#include <unistd.h>
#include <utils.h>

#include <cassert>
#include <iostream>

ThreadPoolLauncher::ThreadPoolLauncher(int pool_sz) : Launcher()
{
    assert(pool_sz > 0);

    int i;
    pthread_t *thread;
    thread_state *states;

    /* Initialize ThreadPoolLauncher fields */
    pool_sz_ = pool_sz;

    /*
     *
     *
     * Initialize num_idle_threads_ (num_idle_threads_mutex_ and num_idle_threads_cond_)
     * and pool_mutex_.
     *
     * pool_mutex_ protects the pool of thread_states, which
     * corresponds to the list of idle threads.
     *
     * num_idle_threads_ is used to track the number of idle/busy processes.
     *
     * When your code is ready, remove the assert(false) statement
     * below.
     */
    num_idle_threads_ = pool_sz;
    pthread_mutex_init(&num_idle_threads_mutex_, NULL);
    pthread_cond_init(&num_idle_threads_cond_, NULL);
    pthread_mutex_init(&pool_mutex_, NULL);
    // assert(false);

    /*
     * Initialize thread_state structs. Each thread in the thread pool is
     * assigned a thread_state struct.
     */
    states = (thread_state *)malloc(sizeof(thread_state) * pool_sz);

    for (i = 0; i < pool_sz; ++i)
    {
        thread = (pthread_t *)malloc(sizeof(pthread_t));

        /*
         * Each thread in the pool has a corresponding semaphore which
         * is used to signal the thread to begin executing a new
         * request. We initialize the value of this semaphore to 0.
         */
        states[i].thread_ready_cond_      = PTHREAD_COND_INITIALIZER;
        states[i].thread_ready_mutex_     = PTHREAD_MUTEX_INITIALIZER;
        states[i].thread_ready_           = false;
        states[i].req_                    = NULL;
        states[i].num_idle_threads_       = &num_idle_threads_;
        states[i].num_idle_threads_mutex_ = &num_idle_threads_mutex_;
        states[i].num_idle_threads_cond_  = &num_idle_threads_cond_;
        states[i].thread_id_              = thread;
        states[i].pool_mutex_             = &pool_mutex_;
        states[i].txns_executed_          = txns_executed_;
        states[i].next_                   = &states[i + 1];
        states[i].pool_                   = &pool_;
    }
    states[i - 1].next_ = &states[0];
    pool_               = states;

    /*
     *
     *
     * Create threads in the thread pool.
     *
     * When your code is ready, remove the assert(false) statement
     * below.
     */
    // assert(false);
    pthread_mutex_lock(&pool_mutex_);
    for (int i = 0; i < pool_sz; i++)
    {
        if (pthread_create(pool_[i].thread_id_, NULL, ExecutorFunc, (void *)&pool_[i]) != 0)
        {
            perror("Thread creation failed");
            exit(EXIT_FAILURE);
        }
    }
    pthread_mutex_unlock(&pool_mutex_);
}

ThreadPoolLauncher::~ThreadPoolLauncher()
{
    int err;

    pthread_mutex_destroy(&num_idle_threads_mutex_);
    err = munmap((void *)&num_idle_threads_mutex_, sizeof(pthread_mutex_t));
    assert(err == 0);

    pthread_cond_destroy(&num_idle_threads_cond_);
    err = munmap((void *)&num_idle_threads_cond_, sizeof(pthread_cond_t));
    assert(err == 0);

    pthread_mutex_destroy(&pool_mutex_);
    err = munmap((void *)&pool_mutex_, sizeof(pthread_mutex_t));
    assert(err == 0);

    while (pool_ != NULL)
    {
        pthread_mutex_destroy(&num_idle_threads_mutex_);
        err = munmap((void *)&num_idle_threads_mutex_, sizeof(pthread_mutex_t));
        assert(err == 0);

        pthread_cond_destroy(&num_idle_threads_cond_);
        err = munmap((void *)&num_idle_threads_cond_, sizeof(pthread_cond_t));
        assert(err == 0);

        pthread_mutex_destroy(&pool_mutex_);
        err = munmap((void *)&pool_mutex_, sizeof(pthread_mutex_t));
        assert(err == 0);

        pthread_join(*pool_->thread_id_, NULL);
        pool_ = pool_->next_;
    }
    free(pool_);
}

void ThreadPoolLauncher::ExecuteRequest(Request *req)
{
    /*
     * Track the number of requests issued.
     */
    num_requests_++;

    /*
     *
     *
     * Find an idle thread from the pool, and execute the request on
     * the idle thread.
     *
     * Hint:
     * 1. Use num_idle_threads_, num_idle_threads_mutex_, and num_idle_threads_cond_
     * 2. Use thread_ready_, thread_ready_mutex_, thread_ready_cond_
     * to coordinate the execution of threads in the pool and the launcher.
     *
     * When your code is ready, remove the assert(false) statement
     * below.
     */
    //  std::cout << num_requests_ << "\n";
    pthread_mutex_lock(&num_idle_threads_mutex_);
    while (num_idle_threads_ == 0)
    {
        pthread_cond_wait(&num_idle_threads_cond_, &num_idle_threads_mutex_);
    }
    num_idle_threads_--;
    pthread_mutex_unlock(&num_idle_threads_mutex_);

    pthread_mutex_lock(&pool_mutex_);
    while (true)
    {
        if (pool_->thread_ready_ == false)
        {
            pool_->thread_ready_ = true;
            pool_->req_          = req;
            pthread_cond_signal(&pool_->thread_ready_cond_);
            pool_ = pool_->next_;
            break;
        }
        else
        {
            pool_ = pool_->next_;
        }
    }
    pthread_mutex_unlock(&pool_mutex_);
}

void *ThreadPoolLauncher::ExecutorFunc(void *arg)
{
    thread_state *st;
    st = (thread_state *)arg;
    while (true)
    {
        /*
         *
         *
         * Wait for a new request, and execute it. After executing the
         * request return thread_state to the launcher's thread_state
         * pool.
         *
         * When your code is ready, remove the assert(false) statement
         * below.
         */

        //  assert(false);
        pthread_mutex_lock(&st->thread_ready_mutex_);
        while (st->thread_ready_ == false)
        {
                        pthread_cond_wait(&st->thread_ready_cond_, &st->thread_ready_mutex_);
        }
        /* exec request */
        //  std::cout << "thread" << *(st->thread_id_) << "    " << *(st->txns_executed_) << "\n";

        st->req_->Execute();
        fetch_and_increment(st->txns_executed_);
        st->thread_ready_ = false;
        pthread_mutex_unlock(&st->thread_ready_mutex_);

        //  std::cout << "finisedthread" << *(st->thread_id_) << "    " << *(st->txns_executed_) << "\n";

        pthread_mutex_lock(st->num_idle_threads_mutex_);
        (*(st->num_idle_threads_))++;
        pthread_cond_signal(st->num_idle_threads_cond_);
        pthread_mutex_unlock(st->num_idle_threads_mutex_);
    }

    return NULL;
}
