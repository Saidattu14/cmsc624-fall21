#include "request.h"

#include <string.h>
#include <unistd.h>

#include <algorithm>
#include <cassert>
#include <iostream>

#define ROUNDUP(n, v) ((n) - 1 + (v) - ((n) - 1) % (v))

Request::Request(Database *db, uint32_t nwrites, uint64_t *writeset, uint64_t *updates)
{
    db_         = db;
    num_writes_ = nwrites;
    writeset_   = writeset;
    updates_    = updates;
}

void Request::SetDatabase(Database *db) { db_ = db; }
size_t Request::CopySize(Request *req)
{
    size_t sz;
    uint32_t nfields;

    nfields = RECORD_SIZE / FIELD_SIZE;
    sz      = sizeof(Request);
    sz      = ROUNDUP(sz, sizeof(uint64_t));
    return sz + sizeof(uint64_t) * (nfields + req->num_writes_);
}

void Request::CopyRequest(char *buf, Request *req)
{
    uint64_t offset;
    uint64_t *array_ptr;
    Request *buf_req;
    uint32_t nfields;

    buf_req = (Request *)buf;

    /* Copy contents of request class */
    memcpy(buf, req, sizeof(Request));

    /* Offset read/write set arrays appropriately */
    offset = ROUNDUP(sizeof(Request), sizeof(uint64_t));
    assert(offset % sizeof(uint64_t) == 0);
    array_ptr = (uint64_t *)&buf[offset];

    /* Copy writeset */
    memcpy(array_ptr, req->writeset_, sizeof(uint64_t) * req->num_writes_);
    buf_req->writeset_ = array_ptr;

    /* Copy updates */
    nfields   = RECORD_SIZE / FIELD_SIZE;
    array_ptr = (array_ptr + req->num_writes_);
    memcpy(array_ptr, req->updates_, sizeof(uint64_t *) * nfields);
    buf_req->updates_ = array_ptr;
}

void Request::DoWrite(char *Record, uint64_t *updates)
{
    uint32_t nfields, i;
    uint64_t *rec_ptr;

    nfields = RECORD_SIZE / FIELD_SIZE;
    for (i = 0; i < nfields; ++i)
    {
        rec_ptr = (uint64_t *)(&Record[i * FIELD_SIZE]);
        *rec_ptr += updates[i];
    }
}

void Request::Execute()
{
    Lock();
    if (isdeadLockFound)
    {
        isdeadLockFound = false;
        sleep(0.01);
        Execute();
    }
    else
    {
        Txn();
        Unlock();
    }
}

void Request::Txn()
{
    assert(RECORD_SIZE % FIELD_SIZE == 0);
    uint32_t i;
    Record *rec;

    for (i = 0; i < num_writes_; ++i)
    {
        if (i > 0) assert(writeset_[i - 1] != writeset_[i]);
        rec = db_->GetRecord(writeset_[i]);
        Request::DoWrite(rec->bytes_, updates_);
    }
}

void Request::Lock()
{
    /*
     * YOUR CODE HERE
     *
     * Hint: How can you ensure that there isn't a deadlock between requests?
     */

    isdeadLockFound = false;
    for (int i = 0; i < num_writes_; i++)
    {
        if (db_->isRecordLocked(writeset_[i]))
        {
            for (int j = 0; j < i; j++)
            {
                db_->UnlockRecord(writeset_[j]);
            }
            isdeadLockFound = true;
            break;
        }
        else
        {
            db_->LockRecord(writeset_[i]);
        }
    }
}

void Request::Unlock()
{
    for (int i = 0; i < num_writes_; ++i)
    {
        db_->UnlockRecord(writeset_[i]);
    }
}
