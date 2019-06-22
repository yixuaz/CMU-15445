/**
 * recovery_manager.h
 * Read log file from disk, redo and undo
 */

#pragma once
#include <algorithm>
#include <mutex>
#include <unordered_map>

#include "buffer/buffer_pool_manager.h"
#include "concurrency/lock_manager.h"
#include "logging/log_record.h"

namespace cmudb {

class LogRecovery {
public:
  LogRecovery(DiskManager *disk_manager,
              BufferPoolManager *buffer_pool_manager)
          : disk_manager_(disk_manager), buffer_pool_manager_(buffer_pool_manager),
            offset_(0) {
    // global transaction through recovery phase
    log_buffer_ = new char[LOG_BUFFER_SIZE];
  }

  ~LogRecovery() {
    delete[] log_buffer_;
    log_buffer_ = nullptr;
  }

  void Redo();
  void Undo();
  bool DeserializeLogRecord(const char *data, LogRecord &log_record);

private:
  // TODO: you can add whatever member variable here
  // Don't forget to initialize newly added variable in constructor
  DiskManager *disk_manager_;
  BufferPoolManager *buffer_pool_manager_;
  // maintain active transactions and its corresponds latest lsn
  std::unordered_map<txn_id_t, lsn_t> active_txn_;
  // mapping log sequence number to log file offset, for undo purpose
  std::unordered_map<lsn_t, int> lsn_mapping_;
  // log buffer related
  int offset_;
  char *log_buffer_;
};

} // namespace cmudb
