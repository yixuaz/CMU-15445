/**
 * log_recovey.cpp
 */

#include "logging/log_recovery.h"
#include "page/table_page.h"

namespace cmudb {
/*
 * deserialize a log record from log buffer
 * @return: true means deserialize succeed, otherwise can't deserialize cause
 * incomplete log record
 */
bool LogRecovery::DeserializeLogRecord(const char *data,
                                       LogRecord &log_record) {
  if (data + LogRecord::HEADER_SIZE > log_buffer_ + LOG_BUFFER_SIZE)
    return false;
  memcpy(&log_record, data, LogRecord::HEADER_SIZE); //COPY HEADER
  if (log_record.size_ <= 0 || data + log_record.size_ > log_buffer_ + LOG_BUFFER_SIZE)
    return false;
  data += LogRecord::HEADER_SIZE;
  switch (log_record.log_record_type_) {
    case LogRecordType::INSERT:
      log_record.insert_rid_ = *reinterpret_cast<const RID *>(data);
      log_record.insert_tuple_.DeserializeFrom(data + sizeof(RID));
      break;
    case LogRecordType::MARKDELETE:
    case LogRecordType::APPLYDELETE:
    case LogRecordType::ROLLBACKDELETE:
      log_record.delete_rid_ = *reinterpret_cast<const RID *>(data);
      log_record.delete_tuple_.DeserializeFrom(data + sizeof(RID));
      break;
    case LogRecordType::UPDATE:
      log_record.update_rid_ = *reinterpret_cast<const RID *>(data);
      log_record.old_tuple_.DeserializeFrom(data + sizeof(RID));
      log_record.new_tuple_.DeserializeFrom(data + sizeof(RID) + 4 + log_record.old_tuple_.GetLength());
      break;
    case LogRecordType::BEGIN:
    case LogRecordType::COMMIT:
    case LogRecordType::ABORT:
      break;
    case LogRecordType::NEWPAGE:
      log_record.prev_page_id_ = *reinterpret_cast<const page_id_t *>(data);
      log_record.page_id_ = *reinterpret_cast<const page_id_t *>(data + sizeof(page_id_t));
      break;
    default:
      assert(false);
  }
  return true;
}

/*
 *redo phase on TABLE PAGE level(table/table_page.h)
 *read log file from the beginning to end (you must prefetch log records into
 *log buffer to reduce unnecessary I/O operations), remember to compare page's
 *LSN with log_record's sequence number, and also build active_txn_ table &
 *lsn_mapping_ table
 */
void LogRecovery::Redo() {
  //lock_guard<mutex> lock(mu_); no thread safe
  // ENABLE_LOGGING must be false when recovery
  assert(ENABLE_LOGGING == false);
  // always replay history from start without checkpoint
  offset_ = 0;
  int bufferOffset = 0;
  while (disk_manager_->ReadLog(log_buffer_ + bufferOffset, LOG_BUFFER_SIZE - bufferOffset, offset_)) {// false means log eof
    int bufferStart = offset_;
    offset_ += LOG_BUFFER_SIZE - bufferOffset;
    bufferOffset = 0;
    LogRecord log;
    while (DeserializeLogRecord(log_buffer_ + bufferOffset, log)) {
      lsn_mapping_[log.GetLSN()] = bufferStart + bufferOffset;
      active_txn_[log.txn_id_] = log.lsn_;
      bufferOffset += log.size_;
      if (log.log_record_type_ == LogRecordType::BEGIN) continue;
      if (log.log_record_type_ == LogRecordType::COMMIT ||
          log.log_record_type_ == LogRecordType::ABORT) {
        assert(active_txn_.erase(log.GetTxnId()) > 0);
        continue;
      }
      if (log.log_record_type_ == LogRecordType::NEWPAGE) {
        auto page = static_cast<TablePage *>(buffer_pool_manager_->FetchPage(log.page_id_));
        assert(page != nullptr);
        bool needRedo = log.lsn_ > page->GetLSN();
        if (needRedo) {
          page->Init(log.page_id_, PAGE_SIZE, log.prev_page_id_, nullptr, nullptr);
          page->SetLSN(log.lsn_);
          if (log.prev_page_id_ != INVALID_PAGE_ID) {
            auto prevPage = static_cast<TablePage *>(
                    buffer_pool_manager_->FetchPage(log.prev_page_id_));
            assert(prevPage != nullptr);
            bool needChange = prevPage->GetNextPageId() == log.page_id_;
            prevPage->SetNextPageId(log.page_id_);
            buffer_pool_manager_->UnpinPage(prevPage->GetPageId(), needChange);
          }
        }
        buffer_pool_manager_->UnpinPage(page->GetPageId(), needRedo);

        continue;
      }
      RID rid = log.log_record_type_ == LogRecordType::INSERT ? log.insert_rid_ :
                log.log_record_type_ == LogRecordType::UPDATE ? log.update_rid_ :
                log.delete_rid_;
      auto page = static_cast<TablePage *>(
              buffer_pool_manager_->FetchPage(rid.GetPageId()));
      assert(page != nullptr);
      bool needRedo = log.lsn_ > page->GetLSN();
      if (needRedo) {
        if (log.log_record_type_ == LogRecordType::INSERT) {
          page->InsertTuple(log.insert_tuple_, rid, nullptr, nullptr, nullptr);
        } else if (log.log_record_type_ == LogRecordType::UPDATE) {
          page->UpdateTuple(log.new_tuple_, log.old_tuple_, rid, nullptr, nullptr, nullptr);
        } else if (log.log_record_type_ == LogRecordType::MARKDELETE) {
          page->MarkDelete(rid, nullptr, nullptr, nullptr);
        } else if (log.log_record_type_ == LogRecordType::APPLYDELETE) {
          page->ApplyDelete(rid, nullptr, nullptr);
        } else if (log.log_record_type_ == LogRecordType::ROLLBACKDELETE) {
          page->RollbackDelete(rid, nullptr, nullptr);
        } else {
          assert(false);//invalid area
        }
        page->SetLSN(log.lsn_);
      }
      buffer_pool_manager_->UnpinPage(page->GetPageId(), needRedo);
    }
    memmove(log_buffer_, log_buffer_ + bufferOffset, LOG_BUFFER_SIZE - bufferOffset);
    bufferOffset = LOG_BUFFER_SIZE - bufferOffset;//rest partial log
  }
}

/*
 *undo phase on TABLE PAGE level(table/table_page.h)
 *iterate through active txn map and undo each operation
 */
void LogRecovery::Undo() {
  //lock_guard<mutex> lock(mu_); no thread safe
  // ENABLE_LOGGING must be false when recovery
  assert(ENABLE_LOGGING == false);
  for (auto &txn : active_txn_) {
    lsn_t lsn = txn.second;
    while (lsn != INVALID_LSN) {
      LogRecord log;
      disk_manager_->ReadLog(log_buffer_, PAGE_SIZE, lsn_mapping_[lsn]);
      assert(DeserializeLogRecord(log_buffer_, log));
      assert(log.lsn_ == lsn);
      lsn = log.prev_lsn_;
      if (log.log_record_type_ == LogRecordType::BEGIN) {
        assert(log.prev_lsn_ == INVALID_LSN);
        continue;
      }
      if (log.log_record_type_ == LogRecordType::COMMIT ||
          log.log_record_type_ == LogRecordType::ABORT) assert(false);
      if (log.log_record_type_ == LogRecordType::NEWPAGE) {
        if (!buffer_pool_manager_->DeletePage(log.page_id_))
          disk_manager_->DeallocatePage(log.page_id_);
        if (log.prev_page_id_ != INVALID_PAGE_ID) {
          auto prevPage = static_cast<TablePage *>(
                  buffer_pool_manager_->FetchPage(log.prev_page_id_));
          assert(prevPage != nullptr);
          assert(prevPage->GetNextPageId() == log.page_id_);
          prevPage->SetNextPageId(INVALID_PAGE_ID);
          buffer_pool_manager_->UnpinPage(prevPage->GetPageId(), true);
        }
        continue;
      }
      RID rid = log.log_record_type_ == LogRecordType::INSERT ? log.insert_rid_ :
                log.log_record_type_ == LogRecordType::UPDATE ? log.update_rid_ :
                log.delete_rid_;
      auto page = static_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
      assert(page != nullptr);
      assert(page->GetLSN() >= log.lsn_);
      if (log.log_record_type_ == LogRecordType::INSERT) {
        page->ApplyDelete(log.insert_rid_, nullptr, nullptr);
      } else if (log.log_record_type_ == LogRecordType::UPDATE) {
        Tuple tuple;
        page->UpdateTuple(log.old_tuple_, tuple, log.update_rid_, nullptr, nullptr, nullptr);
        assert(tuple.GetLength() == log.new_tuple_.GetLength() &&
               memcmp(tuple.GetData(), log.new_tuple_.GetData(), tuple.GetLength()) == 0);
      } else if (log.log_record_type_ == LogRecordType::MARKDELETE) {
        page->RollbackDelete(log.delete_rid_, nullptr, nullptr);
      } else if (log.log_record_type_ == LogRecordType::APPLYDELETE) {
        page->InsertTuple(log.delete_tuple_, log.delete_rid_, nullptr, nullptr, nullptr);
      } else if (log.log_record_type_ == LogRecordType::ROLLBACKDELETE) {
        page->MarkDelete(log.delete_rid_, nullptr, nullptr, nullptr);
      } else assert(false);
      buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    }
  }
  active_txn_.clear();
  lsn_mapping_.clear();
}

} // namespace cmudb
