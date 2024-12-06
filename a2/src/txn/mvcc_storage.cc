#include "mvcc_storage.h"
#include <deque>
// Init the storage
void MVCCStorage::InitStorage()
{
    for (int i = 0; i < 1000000; i++)
    {
        Write(i, 0, 0);
        Mutex* key_mutex = new Mutex();
        mutexs_[i]       = key_mutex;
    }
}

// Free memory.
MVCCStorage::~MVCCStorage()
{
    for (auto it = mvcc_data_.begin(); it != mvcc_data_.end(); ++it)
    {
        delete it->second;
    }

    mvcc_data_.clear();

    for (auto it = mutexs_.begin(); it != mutexs_.end(); ++it)
    {
        delete it->second;
    }

    mutexs_.clear();
}

// Lock the key to protect its version_list. Remember to lock the key when you read/update the version_list
void MVCCStorage::Lock(Key key)
{
    mutexs_[key]->Lock();
}

// Unlock the key.
void MVCCStorage::Unlock(Key key)
{
    mutexs_[key]->Unlock();
}

// MVCC Read
bool MVCCStorage::Read(Key key, Value *result, int txn_unique_id)
{
    //
    // Implement this method!

    // Hint: Iterate the version_lists and return the verion whose write timestamp
    // (version_id) is the largest write timestamp less than or equal to txn_unique_id.

     if (!mvcc_data_.count(key) || (*mvcc_data_[key]).empty()) {
        return false;
    } else {
        deque<Version*>* dq = mvcc_data_[key];
        Version* v = dq->front();
        if(txn_unique_id > v->max_read_id_) {
            *result = v->value_;
            dq->front()->max_read_id_ = txn_unique_id;
        } else {
            for (deque<Version*>::iterator itr = dq->begin(); itr != dq->end(); itr++){
                Version* version = *itr;
                if (version->max_read_id_ > txn_unique_id){
                    break;
                }  else {
                    *result = version->value_;
                }
            }
        }
        return true;
    }
    
}

// Check whether apply or abort the write
bool MVCCStorage::CheckWrite(Key key, int txn_unique_id)
{
    //
    // Implement this method!

    // Hint: Before all writes are applied, we need to make sure that each write
    // can be safely applied based on MVCC timestamp ordering protocol. This method
    // only checks one key, so you should call this method for each key in the
    // write_set. Return true if this key passes the check, return false if not.
    // Note that you don't have to call Lock(key) in this method, just
    // call Lock(key) before you call this method and call Unlock(key) afterward.

     if (!mvcc_data_.count(key) || (*mvcc_data_[key]).empty()) {
        return true;
    } else {
        Version* v =  mvcc_data_[key]->front();
        if(v->max_read_id_ > txn_unique_id) {
          return false;
        }
        return true;
    }
}


bool MVCCStorage::CheckWrite1(Key key, int txn_unique_id)
{
    //
    // Implement this method!

    // Hint: Before all writes are applied, we need to make sure that each write
    // can be safely applied based on MVCC timestamp ordering protocol. This method
    // only checks one key, so you should call this method for each key in the
    // write_set. Return true if this key passes the check, return false if not.
    // Note that you don't have to call Lock(key) in this method, just
    // call Lock(key) before you call this method and call Unlock(key) afterward.

     if (!mvcc_data_.count(key) || (*mvcc_data_[key]).empty()) {
        return true;
    } else {
        Version* v =  mvcc_data_[key]->front();
        if(v->version_id_ > txn_unique_id) {
            return false;
        }
        return true;
    }
}

// MVCC Write, call this method only if CheckWrite return true.
void MVCCStorage::Write(Key key, Value value, int txn_unique_id)
{
    //
    // Implement this method!

    // Hint: Insert a new version (malloc a Version and specify its value/version_id/max_read_id)
    // into the version_lists. Note that InitStorage() also calls this method to init storage.
    // Note that you don't have to call Lock(key) in this method, just
    // call Lock(key) before you call this method and call Unlock(key) afterward.
    // Note that the performance would be much better if you organize the versions in decreasing order.
    Version* version = new Version();
    version->value_ = value;
    version->max_read_id_ = txn_unique_id;
    version->version_id_ = txn_unique_id;
    if (!mvcc_data_[key]) {
        mvcc_data_[key] = new std::deque<Version*>;
    }
    mvcc_data_[key]->push_front(version);
}





// void MVCCStorage::reupdate_read(Key key, int txn_unique_id,int previous_tmp) {
//      if(mvcc_data_[key]) {
//         deque<Version*>* dq = mvcc_data_[key];
//         Version* v = dq->front();
//         if(txn_unique_id == v->max_read_id_) {
//             dq->front()->max_read_id_ = previous_tmp;
//         }
//     }
// }


// #include "txn/mvcc_storage.h"

// // Init the storage
// void MVCCStorage::InitStorage() {
//   for (int i = 0; i < 1000000;i++) {
//     Write(i, 0, 0);
//     Mutex* key_mutex = new Mutex();
//     mutexs_[i] = key_mutex;
//   }
// }

// // Free memory.
// MVCCStorage::~MVCCStorage() {
//   for (unordered_map<Key, deque<Version*>*>::iterator it = mvcc_data_.begin();
//        it != mvcc_data_.end(); ++it) {
//     delete it->second;          
//   }
  
//   mvcc_data_.clear();
  
//   for (unordered_map<Key, Mutex*>::iterator it = mutexs_.begin();
//        it != mutexs_.end(); ++it) {
//     delete it->second;          
//   }
  
//   mutexs_.clear();
// }

// // Lock the key to protect its version_list. Remember to lock the key when you read/update the version_list 
// void MVCCStorage::Lock(Key key) {
//   mutexs_[key]->Lock();
// }

// // Unlock the key.
// void MVCCStorage::Unlock(Key key) {
//   mutexs_[key]->Unlock();
// }

// // MVCC Read
// bool MVCCStorage::Read(Key key, Value* result, int txn_unique_id,int* previous_tmp) {
//   // CPSC 638:
// 	//
//   // Hint: Iterate the version_lists and return the verion whose write timestamp
//   // (version_id) is the largest write timestamp less than or equal to txn_unique_id.
  
//   //key not found
//   if (!mvcc_data_.count(key)){
//     return false;
//   }

//   deque<Version*> *data = mvcc_data_[key];
//   if (data->empty()) {
//     //no data
//     return false;
//   }
//   else {
//     int maxlessthan = 0;
//     deque<Version*> version_list = *data;
//     for (deque<Version*>::iterator itr = version_list.begin(); itr != version_list.end(); itr++){
//         Version* version = *itr;
//         //get largest less than version
//         if (version->version_id_ <= txn_unique_id && version->version_id_ > maxlessthan){
//           //set new version
//           *result = version->value_;
//           version->max_read_id_ = txn_unique_id;
//         }  
//     }
//   }
//   return true;
// }



// // Check whether apply or abort the write
// bool MVCCStorage::CheckWrite(Key key, int txn_unique_id) {
//   // CPSC 638:
//   //
//   // Implement this method!
	
//   // Hint: Before all writes are applied, we need to make sure that each write
//   // can be safely applied based on MVCC timestamp ordering protocol. This method
//   // only checks one key, so you should call this method for each key in the
//   // write_set. Return true if this key passes the check, return false if not. 
//   // Note that you don't have to call Lock(key) in this method, just
//   // call Lock(key) before you call this method and call Unlock(key) afterward.
  
//   //key not found
//   if (!mvcc_data_.count(key)){
//     return false;
//   }

//   deque<Version*> *data = mvcc_data_[key];
  
//   if (data->empty()) {
//     //no data
//     return true;
//   }
//   else {
//     deque<Version*> version_list = *data;
//     //get latest version
//     Version* version = version_list.back();

//     //check if version valid
//     if (version->max_read_id_ > txn_unique_id){
//       return false;
//     }
//   }
//   return true;
// }

// // MVCC Write, call this method only if CheckWrite return true.
// void MVCCStorage::Write(Key key, Value value, int txn_unique_id) {
//   // CPSC 638:
//   //
//   // Implement this method!
  
//   // Hint: Insert a new version (malloc a Version and specify its value/version_id/max_read_id)
//   // into the version_lists. Note that InitStorage() also calls this method to init storage. 
//   // Note that you don't have to call Lock(key) in this method, just
//   // call Lock(key) before you call this method and call Unlock(key) afterward.

//   //create new version
//   Version* new_update = new Version();
// 	new_update->value_ 				= value;
// 	new_update->version_id_ 	= txn_unique_id;
// 	new_update->max_read_id_ 	= 0;

// 	deque<Version*>* versions;

// 	if(mvcc_data_.count(key) > 0) {
//     //key exists
// 		versions = mvcc_data_[key];
// 	}
//   else
//   {
//     //key not exists
// 		versions = new deque<Version*>();
// 		mvcc_data_[key] = versions;
// 	}

//   //push to deque
// 	versions->push_front(new_update);
// }



// void MVCCStorage::reupdate_read(Key key, int txn_unique_id,int previous_tmp) {
//      if(mvcc_data_[key]) {
//         deque<Version*>* dq = mvcc_data_[key];
//         Version* v = dq->front();
//         if(txn_unique_id == v->max_read_id_) {
//             dq->front()->max_read_id_ = previous_tmp;
//         }
//     }
// }



// #include "txn/mvcc_storage.h"

// // Init the storage
// void MVCCStorage::InitStorage() {
//   // std::cout <<"masuk init mvcc\n" << std::flush;
//   for (int i = 0; i < 1000000;i++) {
//     Write(i, 0, 0);
//     Mutex* key_mutex = new Mutex();
//     mutexs_[i] = key_mutex;
//   }
// }

// // Free memory.
// MVCCStorage::~MVCCStorage() {
//   for (unordered_map<Key, deque<Version*>*>::iterator it = mvcc_data_.begin();
//        it != mvcc_data_.end(); ++it) {
//     delete it->second;          
//   }
  
//   mvcc_data_.clear();
  
//   for (unordered_map<Key, Mutex*>::iterator it = mutexs_.begin();
//        it != mutexs_.end(); ++it) {
//     delete it->second;          
//   }
  
//   mutexs_.clear();
// }

// // Lock the key to protect its version_list. Remember to lock the key when you read/update the version_list 
// void MVCCStorage::Lock(Key key) {
//   mutexs_[key]->Lock();
// }

// // Unlock the key.
// void MVCCStorage::Unlock(Key key) {
//   mutexs_[key]->Unlock();
// }

// // MVCC Read
// bool MVCCStorage::Read(Key key, Value* result, int txn_unique_id,int *ii) {
//   // Implement this method!
  
//   // Hint: Iterate the version_lists and return the verion whose write timestamp
//   // (version_id) is the largest write timestamp less than or equal to txn_unique_id.

//   if (!mvcc_data_.count(key) || (*mvcc_data_[key]).empty()) return false;

//   int max_version_id = 0;
//   Version* selected_version;

//   // Find max version
//   for (auto& it : *mvcc_data_[key]) {
//     if (it->version_id_ < max_version_id) continue;
//     if (it->version_id_ <= txn_unique_id){
//       *result = it->value_;
//       max_version_id = it->version_id_;
//       selected_version = it;
//     }
//   }

//   // Update read timestamp of selected version
//   if((max_version_id > 0) && (selected_version->max_read_id_ < txn_unique_id)){
//     selected_version->max_read_id_ = txn_unique_id;  
//   }
//   return (max_version_id > 0);
// }


// // Check whether apply or abort the write
// bool MVCCStorage::CheckWrite(Key key, int txn_unique_id) {
//   // Implement this method!
  
//   // Hint: Before all writes are applied, we need to make sure that each write
//   // can be safely applied based on MVCC timestamp ordering protocol. This method
//   // only checks one key, so you should call this method for each key in the
//   // write_set. Return true if this key passes the check, return false if not. 
//   // Note that you don't have to call Lock(key) in this method, just
//   // call Lock(key) before you call this method and call Unlock(key) afterward.

//   if (!mvcc_data_.count(key) || (*mvcc_data_[key]).empty()) return false;

//   int max_version_id = 0;
//   int max_read_id = 0;

//   // iterate for max_version id
//   for (auto it : *mvcc_data_[key]) {
//     if (it->version_id_ < max_version_id) continue;
//     if (it->version_id_ <= txn_unique_id){
//           max_version_id = it->version_id_;
//           max_read_id = it->max_read_id_;
//     }
//   }

//   // Ti want to write, and Tk exist
//   // RTS(Ti) < RTS(Tk)
//   return (max_read_id <= txn_unique_id);
// }

// // MVCC Write, call this method only if CheckWrite return true.
// void MVCCStorage::Write(Key key, Value value, int txn_unique_id) {
//   // Implement this method!
  
//   // Hint: Insert a new version (malloc a Version and specify its value/version_id/max_read_id)
//   // into the version_lists. Note that InitStorage() also calls this method to init storage. 
//   // Note that you don't have to call Lock(key) in this method, just
//   // call Lock(key) before you call this method and call Unlock(key) afterward.
//   // Note that the performance would be much better if you organize the versions in decreasing order.
//   Version* new_version = new Version;
  
//   new_version->value_ = value;
//   new_version->version_id_ = txn_unique_id;
//   new_version->max_read_id_ = txn_unique_id;
  
//   if (mvcc_data_.count(key)) {
//     int max_version_id = 0;
//     Version* selected_version;
//     // iterate for max_version id
//     // searching for Qk
//     for (auto& it : *mvcc_data_[key]) {
//       if (it->version_id_ < max_version_id) continue;
//       if (it->version_id_ <= txn_unique_id){
//         max_version_id = it->version_id_;
//         selected_version = it;
//       }
//     }

//     // Update value of selected version
//     // if Qk exists and it has equal timestamp with current timestamp, update its value
//     if((max_version_id > 0) && (selected_version->version_id_ == txn_unique_id)){
//       selected_version->value_ = value;
//       return;
//     }

//   } else {
//     mvcc_data_[key] = new std::deque<Version*>;
//   }

//   // mvcc_data_[key]->push_back(new_version);
//   mvcc_data_[key]->push_front(new_version);
// }

// #include "txn/mvcc_storage.h"

// // Init the storage
// void MVCCStorage::InitStorage() {
//   for (int i = 0; i < 1000000;i++) {
//     Write(i, 0, 0);
//     Mutex* key_mutex = new Mutex();
//     mutexs_[i] = key_mutex;
//     // std::cout << "inside loop" << std::endl << std::flush;

//   }
//   // std::cout << "Test init storage" << std::endl << std::flush;
// }

// // Free memory.
// MVCCStorage::~MVCCStorage() {
//   for (unordered_map<Key, deque<Version*>*>::iterator it = mvcc_data_.begin();
//        it != mvcc_data_.end(); ++it) {
//     delete it->second;          
//   }
  
//   mvcc_data_.clear();
  
//   for (unordered_map<Key, Mutex*>::iterator it = mutexs_.begin();
//        it != mutexs_.end(); ++it) {
//     delete it->second;          
//   }
  
//   mutexs_.clear();
// }

// // Lock the key to protect its version_list. Remember to lock the key when you read/update the version_list 
// void MVCCStorage::Lock(Key key) {
//   mutexs_[key]->Lock();
// }

// // Unlock the key.
// void MVCCStorage::Unlock(Key key) {
//   mutexs_[key]->Unlock();
// }

// // MVCC Read
// bool MVCCStorage::Read(Key key, Value* result, int txn_unique_id,int *i) {
//   // CPSC 438/538:
//   //
//   // Implement this method!
  
//   // Hint: Iterate the version_lists and return the verion whose write timestamp
//   // (version_id) is the largest write timestamp less than or equal to txn_unique_id.
//   bool found = false;
//   if (mvcc_data_.count(key) && !(*mvcc_data_[key]).empty()){ 
//     deque<Version*> versions = *mvcc_data_[key];
//     int temp = -1;
//     for (deque<Version*>::iterator it=versions.begin(); it!=versions.end(); ++it){
//       if (((*it)->version_id_ > temp) && ((*it)->version_id_<=txn_unique_id)){
//         temp = (*it)->version_id_;
//         *result = (*it)->value_;
//         found = true;
//       }
//     }
//     for (deque<Version*>::iterator it=versions.begin(); it!=versions.end(); ++it){
//       if (((*it)->max_read_id_ < txn_unique_id) && (temp == (*it)->version_id_)) {
//       (*it)->max_read_id_ = txn_unique_id;
//         break;
//       }
//     }
//   }
//   // std::cout << "Read found : " << found << std::endl; 
//   return found;
// }


// // Check whether apply or abort the write
// bool MVCCStorage::CheckWrite(Key key, int txn_unique_id) {
//   // CPSC 438/538:
//   //
//   // Implement this method!
  
//   // Hint: Before all writes are applied, we need to make sure that each write
//   // can be safely applied based on MVCC timestamp ordering protocol. This method
//   // only checks one key, so you should call this method for each key in the
//   // write_set. Return true if this key passes the check, return false if not. 
//   // Note that you don't have to call Lock(key) in this method, just
//   // call Lock(key) before you call this method and call Unlock(key) afterward.
//   if (mvcc_data_.count(key) && !(*mvcc_data_[key]).empty()){
//     deque<Version*> versions = *mvcc_data_[key];
//     int temp_write = -1;
//     int temp_read = -1;
//     for (deque<Version*>::iterator it=versions.begin(); it!=versions.end(); ++it){
//       if (((*it)->version_id_ > temp_write) && ((*it)->version_id_<=txn_unique_id)){
//         temp_write = (*it)->version_id_;
//         temp_read = (*it)->max_read_id_;
//       }
//     }
//     if (temp_read > txn_unique_id){
//       return false;
//     }
//     else{
//       return true;
//     }
//   }
//   else {
//     return true;
//   }

// }

// // MVCC Write, call this method only if CheckWrite return true.
// void MVCCStorage::Write(Key key, Value value, int txn_unique_id) {
//   // CPSC 438/538:
//   //
//   // Implement this method!
  
//   // Hint: Insert a new version (malloc a Version and specify its value/version_id/max_read_id)
//   // into the version_lists. Note that InitStorage() also calls this method to init storage. 
//   // Note that you don't have to call Lock(key) in this method, just
//   // call Lock(key) before you call this method and call Unlock(key) afterward.
//   // std::cout << "Test write" << std::endl << std::flush;
//   if (!mvcc_data_.count(key)){
//     // std::cout << "Test write no key exist in mvcc data" << std::endl << std::flush;
//     deque<Version*>* versions = new deque<Version*>;
//     mvcc_data_[key] = versions;
//     // std::cout << "Test write add key and value" << std::endl << std::flush;

//     Version *new_ver = new Version;
//     (new_ver)->value_ = value;
//     (new_ver)->max_read_id_ = txn_unique_id;
//     (new_ver)->version_id_ = txn_unique_id;
//     mvcc_data_[key]->push_back(new_ver);
//     // std::cout << mvcc_data_[key]->size() << std::endl << std::flush;
//     }
//   else {
//     deque<Version*> versions = *mvcc_data_[key];
//     if (!versions.empty()){
//       int temp = -1;
//       for (deque<Version*>::iterator it=versions.begin(); it!=versions.end(); ++it){
//         if (((*it)->version_id_ > temp) && ((*it)->version_id_<=txn_unique_id)){
//           temp = (*it)->version_id_;
//         }
//       }
//       for (deque<Version*>::iterator it=versions.begin(); it!=versions.end(); ++it){
//         if (temp == (*it)->version_id_){
//           if (txn_unique_id == (*it)->version_id_){
//             (*it)->value_ = value;
//           }
//           else{
//             Version *new_ver = new Version;
//             new_ver->value_ = value;
//             new_ver->max_read_id_ = txn_unique_id;
//             new_ver->version_id_ = txn_unique_id;
//             mvcc_data_[key]->push_back(new_ver);
//           }
//           break;
//         }
//       }
//     }
//     else {
//       Version *new_ver = new Version;
//       new_ver->value_ = value;
//       new_ver->max_read_id_ = txn_unique_id;
//       new_ver->version_id_ = txn_unique_id;
//       (mvcc_data_)[key]->push_back(new_ver);
//     }

//   }
// }


// #include "txn/mvcc_storage.h"

// // Fetch largest write timestamp (version_id_) and return it.
// // Precondition: Key already exists in a map.
// int MVCCStorage::FetchLargestVersionID(Key key, int txn_unique_id) {
//     int max_version_id_= -99999;
//     if (mvcc_data_.find(key) == mvcc_data_.end()) {
//         return -99999;
//     }
//     for (deque<Version*>::iterator it = mvcc_data_[key]->begin(); it != mvcc_data_[key]->end(); ++it) {
//         if ((*it)->version_id_ <= txn_unique_id && (*it)->version_id_ > max_version_id_) {
//             max_version_id_ = (*it)->version_id_;
//         }
//     }
//     return max_version_id_;
// }

// // Init the storage
// void MVCCStorage::InitStorage() {
// 	for (int i = 0; i < 1000000;i++) {
//     	Write(i, 0, 0);
//     	Mutex* key_mutex = new Mutex();
//     	mutexs_[i] = key_mutex;
//   	}
// }

// // Free memory.
// MVCCStorage::~MVCCStorage() {
// 	for (unordered_map<Key, deque<Version*>*>::iterator it = mvcc_data_.begin(); it != mvcc_data_.end(); ++it) {
//     	// std::cout << "key: " << it->first << std::endl;
// 		// for (deque<Version*>::iterator itr = mvcc_data_[it->first]->begin(); itr != mvcc_data_[it->first]->end(); ++it) {
// 			// std::cout << "itr value: " << (*itr)->version_id_ << std::endl;
// 			// delete (*itr);
// 		// }
// 		// std::cout << "attempt to delete: " << std::endl;
// 		delete it->second;
//   }
// 	mvcc_data_.clear();
  
//   for (unordered_map<Key, Mutex*>::iterator it = mutexs_.begin();
//        it != mutexs_.end(); ++it) {
//     delete it->second;          
//   }
  
//   mutexs_.clear();
// }

// // Lock the key to protect its version_list. Remember to lock the key when you read/update the version_list 
// void MVCCStorage::Lock(Key key) {
//   mutexs_[key]->Lock();
// }

// // Unlock the key.
// void MVCCStorage::Unlock(Key key) {
//   mutexs_[key]->Unlock();
// }

// // MVCC Read
// bool MVCCStorage::Read(Key key, Value* result, int txn_unique_id,int *t) {
//   // CPSC 438/538:
//   //
//   // Implement this method!
//   // Hint: Iterate the version_lists and return the verion whose write timestamp
//   // (version_id) is the largest write timestamp less than or equal to txn_unique_id.

//   // Check if key exists
//   if (mvcc_data_.find(key) == mvcc_data_.end()) {
//     return false;
//   }

//   // Fetch largest version id
//   int max_version_id_ = FetchLargestVersionID(key, txn_unique_id);
  
//   // If no write timestamp that is less than or equal to txn_unique_id
//   if (max_version_id_ == -99999) {
//     return false;
//   }

//   // Fetch the value and update the max_read_id.
//   for (deque<Version*>::iterator it = mvcc_data_[key]->begin(); it != mvcc_data_[key]->end(); ++it) {
//     if (max_version_id_ == (*it)->version_id_) {
//         *result = (*it)->value_;
//         // Overwrite  largest max_read_id_
//         if (txn_unique_id > (*it)->max_read_id_) {
//         	(*it)->max_read_id_ = txn_unique_id;
//         }
//             break;
//         }
//   }
//   return true;
// }


// // Check whether apply or abort the write
// bool MVCCStorage::CheckWrite(Key key, int txn_unique_id) {
//   // CPSC 438/538:
//   //
//   // Implement this method!
  
//   // Hint: Before all writes are applied, we need to make sure that each write
//   // can be safely applied based on MVCC timestamp ordering protocol. This method
//   // only checks one key, so you should call this method for each key in the
//   // write_set. Return true if this key passes the check, return false if not. 
//   // Note that you don't have to call Lock(key) in this method, just
//   // call Lock(key) before you call this method and call Unlock(key) afterward.
  
//   // Fetch largest version id
//   int max_version_id_ = FetchLargestVersionID(key, txn_unique_id);
//   // If no write timestamp that is less than or equal to txn_unique_id
//   if (max_version_id_ == -99999) {
//     return true;
//   }
//   for (deque<Version*>::iterator it = mvcc_data_[key]->begin(); it != mvcc_data_[key]->end(); ++it) {
//     if (max_version_id_ == (*it)->version_id_) {
//         // Check read timestamp
//         if (txn_unique_id < (*it)->max_read_id_) {
//           return false;
//         }
//         break;
//     }
//   }
//   return true;
// }

// // MVCC Write, call this method only if CheckWrite return true.
// void MVCCStorage::Write(Key key, Value value, int txn_unique_id) {
//     // CPSC 438/538:
//     //
//     // Implement this method!
//     // Hint: Insert a new version (malloc a Version and specify its value/version_id/max_read_id)
//     // into the version_lists. Note that InitStorage() also calls this method to init storage. 
//     // Note that you don't have to call Lock(key) in this method, just
//     // call Lock(key) before you call this method and call Unlock(key) afterward.
    
//     // Fetch largest version id.
//     Version* new_version = new Version;
//     int max_version_id_ = FetchLargestVersionID(key, txn_unique_id);
// 	// std::cout << "max_version_id: " << max_version_id_ << std::endl;
//     // If is empty or not found, write
//     if (max_version_id_ == -99999) {
//         // If key does not exist then
//         if (mvcc_data_.find(key) == mvcc_data_.end()) {
//             // Allocate a new deque
//             deque<Version*>* deque = new std::deque<Version*>;
//             mvcc_data_[key] = deque;
//         }
//         new_version->max_read_id_ = txn_unique_id;
//         new_version->value_ = value;
//         new_version->version_id_ = txn_unique_id;
//         mvcc_data_[key]->push_back(new_version);
//         return;
//       }
//       for (deque<Version*>::iterator it = mvcc_data_[key]->begin(); it != mvcc_data_[key]->end(); ++it) {
//           if (max_version_id_ == (*it)->version_id_) {
//               if (max_version_id_ == txn_unique_id) {
//                   // Overwrite
//                   (*it)->value_ = value;
//               } else {
//                   // Allocation of new version.
//                   new_version->max_read_id_ = txn_unique_id;
//                   new_version->value_ = value;
//                   new_version->version_id_ = txn_unique_id;
//                   mvcc_data_[key]->push_back(new_version);
//               }
//             return;
//           }
//       }
// }