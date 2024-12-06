#ifndef REQUEST_H_
#define REQUEST_H_

#include <pthread.h>

#include <cstdint>
#include <vector>

#include "db/database.h"

class Request
{
   private:
    Request();
    Request(const Request &);
    Request &operator=(const Request &);

   protected:
    Database *db_;
    uint32_t num_writes_;
    uint64_t *writeset_;
    uint64_t *updates_;
    bool isdeadLockFound = false;
    static void DoWrite(char *Record, uint64_t *updates);

    void Lock();
    void Txn();
    void Unlock();

   public:
    Request(Database *db, uint32_t nwrites, uint64_t *writeset, uint64_t *updates);
    static void CopyRequest(char *buf, Request *req);
    static size_t CopySize(Request *req);
    void Execute();
    void SetDatabase(Database *db);
};

#endif  // REQUEST_H_
