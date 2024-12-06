#include "process_pool_launcher.h"

#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>
#include <utils.h>

#include <cassert>
#include <iostream>

ProcessPoolLauncher::ProcessPoolLauncher(uint32_t nprocs) : Launcher()
{
    uint32_t i;
    char *req_bufs;
    proc_state *pstates;

    /*
     * Setup launcher state. Initialize the launcher's proc_mgr struct.
     */
    launcher_state_ = (proc_mgr *)mmap(NULL, sizeof(proc_mgr), PROT_FLAGS, MAP_FLAGS, 0, 0);

    /*
     *
     *
     * Initialize launcher_state_->num_idle_procs_, num_idle_procs_mutex_ and
     * num_idle_procs_cond_ with an appropriate starting value/attributes.
     * num_idle_procs_ is used to track the number of idle processes.
     * ProcessPoolLauncher's use of num_idle_procs_ is similar to
     * ProcessLauncher's use of max_outstanding_ to control the number
     * of outstanding processes.
     *
     * Hint: Since they are shared among multiple processes, their
     * argument must be set appropriately.
     *
     * When your code is ready, remove the assert(false) statement
     * below.
     */
    launcher_state_->num_idle_procs_ = (volatile uint32_t *)mmap(NULL, sizeof(uint32_t), PROT_FLAGS, MAP_FLAGS, 0, 0);
    memset((void *)launcher_state_->num_idle_procs_, 0x0, sizeof(uint32_t));
    *launcher_state_->num_idle_procs_ = nprocs;

    launcher_state_->num_idle_procs_mutex_ =
        (pthread_mutex_t *)mmap(NULL, sizeof(pthread_mutex_t), PROT_FLAGS, MAP_FLAGS, 0, 0);
    pthread_mutexattr_t mutexattr;
    pthread_mutexattr_init(&mutexattr);
    pthread_mutexattr_setpshared(&mutexattr, PTHREAD_PROCESS_SHARED);
    pthread_mutex_init(launcher_state_->num_idle_procs_mutex_, &mutexattr);

    launcher_state_->num_idle_procs_cond_ =
        (pthread_cond_t *)mmap(NULL, sizeof(pthread_cond_t), PROT_FLAGS, MAP_FLAGS, 0, 0);
    pthread_condattr_t condattr;
    pthread_condattr_init(&condattr);
    pthread_condattr_setpshared(&condattr, PTHREAD_PROCESS_SHARED);
    pthread_cond_init(launcher_state_->num_idle_procs_cond_, &condattr);

    // assert(false);

    /*
     *
     *
     * pool_mutex_ is used as a mutex lock. It protects the pool of
     * idle processes from concurrent modifications.
     * Initialize pool_mutex_ with an appropriate starting value.
     *
     * Hint: Since pool_mutex_ is shared among multiple processes, its
     * second argument must be set appropriately.
     *
     * When your code is ready, remove the assert(false) statement
     * below.
     */
    launcher_state_->pool_mutex_ = (pthread_mutex_t *)mmap(NULL, sizeof(pthread_mutex_t), PROT_FLAGS, MAP_FLAGS, 0, 0);
    pthread_mutexattr_t mutexattr1;
    pthread_mutexattr_init(&mutexattr1);
    pthread_mutexattr_setpshared(&mutexattr1, PTHREAD_PROCESS_SHARED);
    pthread_mutex_init(launcher_state_->pool_mutex_, &mutexattr1);

    // assert(false);

    /*
     * Setup request buffers. Each process in the pool has its own private
     * request buffer. In order to assign a request to a process the
     * launcher process must copy the request into the process' request
     * buffer.
     */
    req_bufs = (char *)mmap((NULL), nprocs * RQST_BUF_SZ, PROT_FLAGS, MAP_FLAGS, 0, 0);

    /*
     *
     *
     * Each process in the pool has a corresponding mutex, conditional variable
     * and value (procs_ready) which are used to signal the process to begin executing
     * a new request.
     *
     * Initialize procs_ready, procs_mutex and procs_cond
     *
     * When your code is ready, remove the assert(false) statement
     * below.
     */
    bool *procs_ready            = NULL;
    pthread_mutex_t *procs_mutex = NULL;
    pthread_cond_t *procs_cond   = NULL;
    procs_ready                  = (bool *)mmap(NULL, sizeof(bool) * nprocs, PROT_FLAGS, MAP_FLAGS, 0, 0);
    procs_mutex = (pthread_mutex_t *)mmap(NULL, sizeof(pthread_mutex_t) * nprocs, PROT_FLAGS, MAP_FLAGS, 0, 0);
    procs_cond  = (pthread_cond_t *)mmap(NULL, sizeof(pthread_cond_t) * nprocs, PROT_FLAGS, MAP_FLAGS, 0, 0);
    for (i = 0; i < nprocs; ++i)
    {
        procs_ready[i] = false;
        pthread_mutexattr_t mutexattr1;
        pthread_mutexattr_init(&mutexattr1);
        pthread_mutexattr_setpshared(&mutexattr1, PTHREAD_PROCESS_SHARED);
        pthread_mutex_init(&procs_mutex[i], &mutexattr1);

        pthread_condattr_t condattr1;
        pthread_condattr_init(&condattr1);
        pthread_condattr_setpshared(&condattr1, PTHREAD_PROCESS_SHARED);
        pthread_cond_init(&procs_cond[i], &condattr1);
    }

    // assert(false);

    /*
     * Setup proc_states for each process in the pool. proc_states are
     * linked via the next_ field. The free-list of proc_states is
     * stored in launcher_state_->pool_.
     */
    pstates = (proc_state *)mmap(NULL, sizeof(proc_state) * nprocs, PROT_FLAGS, MAP_FLAGS, 0, 0);
    for (i = 0; i < nprocs; ++i)
    {
        pstates[i].request_        = (Request *)&req_bufs[i * RQST_BUF_SZ];
        pstates[i].proc_ready_     = &procs_ready[i];
        pstates[i].proc_mutex_     = &procs_mutex[i];
        pstates[i].proc_cond_      = &procs_cond[i];
        pstates[i].launcher_state_ = launcher_state_;
        pstates[i].txns_executed_  = txns_executed_;
        pstates[i].next_           = &pstates[i + 1];
    }
    pstates[i - 1].next_   = &pstates[0];
    launcher_state_->pool_ = pstates;

    /*
     *
     *
     * Launch the processes in the process pool.
     * Also, remember to update pool_sz_
     *
     * When your code is ready, remove the assert(false) statement
     * below.
     */
    pool_sz_ = nprocs;
    pthread_mutex_lock(launcher_state_->pool_mutex_);
    for (int i1 = 0; i1 < nprocs; i1++)
    {
        pid_t pid;
        pid = fork();
        if (pid == 0)
        {
            ExecutorFunc(&launcher_state_->pool_[i1]);
        }
    }
    pthread_mutex_unlock(launcher_state_->pool_mutex_);
    // assert(false);
}

ProcessPoolLauncher::~ProcessPoolLauncher()
{
    int err;
    err = munmap((void *)launcher_state_->num_idle_procs_, sizeof(uint32_t));
    assert(err == 0);

    pthread_mutex_destroy(launcher_state_->num_idle_procs_mutex_);
    err = munmap((void *)launcher_state_->num_idle_procs_mutex_, sizeof(pthread_mutex_t));
    assert(err == 0);

    pthread_cond_destroy(launcher_state_->num_idle_procs_cond_);
    err = munmap((void *)launcher_state_->num_idle_procs_cond_, sizeof(pthread_cond_t));
    assert(err == 0);

    pthread_mutex_destroy(launcher_state_->pool_mutex_);
    err = munmap((void *)launcher_state_->pool_mutex_, sizeof(pthread_mutex_t));
    assert(err == 0);

    proc_state *pstate = launcher_state_->pool_;
    while (pstate != NULL)
    {
        err = munmap((void *)pstate->proc_ready_, sizeof(bool));
        assert(err == 0);

        pthread_mutex_destroy(pstate->proc_mutex_);
        err = munmap((void *)pstate->proc_mutex_, sizeof(pthread_mutex_t));
        assert(err == 0);

        pthread_cond_destroy(pstate->proc_cond_);
        err = munmap((void *)pstate->proc_cond_, sizeof(pthread_cond_t));
        assert(err == 0);

        err = munmap((void *)pstate->request_, RQST_BUF_SZ);
        assert(err == 0);

        proc_state *next = pstate->next_;
        err              = munmap((void *)pstate, sizeof(proc_state));
        assert(err == 0);
        pstate = next;
    }

    err = munmap((void *)launcher_state_, sizeof(proc_mgr));
    assert(err == 0);
}

void ProcessPoolLauncher::ExecutorFunc(proc_state *st)
{
    while (true)
    {
        /*
         *
         *
         * Wait for a new request, and execute it. After executing the
         * request return proc_state to the launcher's proc_state
         * pool. Remember to signal/wait the appropriate proc_ready_,
         * and num_idle_procs_.
         *
         * When your code is ready, remove the assert(false) statement
         * below.
         */

        // assert(false);
        pthread_mutex_lock(st->proc_mutex_);
        while (*st->proc_ready_ == false)
        {
            pthread_cond_wait(st->proc_cond_, st->proc_mutex_);
        }
        st->request_->Execute();
        fetch_and_increment(st->txns_executed_);
        *st->proc_ready_ = false;
        pthread_mutex_unlock(st->proc_mutex_);

        // st->launcher_state_->pool_mutex_
        pthread_mutex_lock(st->launcher_state_->num_idle_procs_mutex_);
        *(st->launcher_state_->num_idle_procs_)++;
        pthread_cond_signal(st->launcher_state_->num_idle_procs_cond_);
        // std::cout << st->launcher_state_->num_idle_procs_ << "dj\n";
        pthread_mutex_unlock(st->launcher_state_->num_idle_procs_mutex_);
        /*
         *
         *
         * When your code is ready, remove the assert(false) statement
         * below.
         */
        // assert(false);
    }
    exit(0);
}

void ProcessPoolLauncher::ExecuteRequest(Request *req)
{
    proc_state *st;
    /*
     * Track the number of requests issued.
     */
    num_requests_++;

    st = launcher_state_->pool_;

    /*
     *
     *
     * Find an idle process from pool_, **copy** the request into the
     * process' request buffer, and execute the request on the idle process.
     *
     * Hint: Use the process' proc_ready_, and the launcher's num_idle_procs_
     * to initiate a new request on the idle process, and ensure that there
     * exist idle processes.
     *
     * Hint: Use the API from txn/request.h to copy the request.
     *
     * When your code is ready, remove the assert(false) statement
     * below.
     */
    // std::cout << num_requests_ << "\n";
    pthread_mutex_lock(st->launcher_state_->num_idle_procs_mutex_);
    while (*st->launcher_state_->num_idle_procs_ == 0)
    {
        pthread_cond_wait(st->launcher_state_->num_idle_procs_cond_, st->launcher_state_->num_idle_procs_mutex_);
    }
    *(st->launcher_state_->num_idle_procs_)--;
    pthread_mutex_unlock(st->launcher_state_->num_idle_procs_mutex_);

    pthread_mutex_lock(st->launcher_state_->pool_mutex_);
    while (true)
    {
        if (*st->proc_ready_ == false)
        {
            *st->proc_ready_ = true;
            st->request_->CopyRequest((char *)st->request_, req);
            pthread_cond_signal(st->proc_cond_);
            break;
        }
        else
        {
            st = st->next_;
        }
    }
    pthread_mutex_unlock(st->launcher_state_->pool_mutex_);
    // assert(false);
}
