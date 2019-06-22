/**
 * log_record.h
 * For every write opeartion on table page, you should write ahead a
 * corresponding log record.
 * For EACH log record, HEADER is like (5 fields in common, 20 bytes in totoal)
 *-------------------------------------------------------------
 * | size | LSN | transID | prevLSN | LogType |
 *-------------------------------------------------------------
 * For insert type log record
 *-------------------------------------------------------------
 * | HEADER | tuple_rid | tuple_size | tuple_data(char[] array) |
 *-------------------------------------------------------------
 * For delete type(including markdelete, rollbackdelete, applydelete)
 *-------------------------------------------------------------
 * | HEADER | tuple_rid | tuple_size | tuple_data(char[] array) |
 *-------------------------------------------------------------
 * For update type log record
 *------------------------------------------------------------------------------
 * | HEADER | tuple_rid | tuple_size | old_tuple_data | tuple_size |
 * | new_tuple_data |
 *------------------------------------------------------------------------------
 * For new page type log record
 *-------------------------------------------------------------
 * | HEADER | prev_page_id | page_id
 *-------------------------------------------------------------
 */
#pragma once
#include <cassert>

#include "common/config.h"
#include "table/tuple.h"

namespace cmudb {
// log record type
enum class LogRecordType {
  INVALID = 0,
  INSERT,
  MARKDELETE,
  APPLYDELETE,
  ROLLBACKDELETE,
  UPDATE,
  BEGIN,
  COMMIT,
  ABORT,
  // when create a new page in heap table
          NEWPAGE,
};

class LogRecord {
  friend class LogManager;
  friend class LogRecovery;

public:
  LogRecord()
          : size_(0), lsn_(INVALID_LSN), txn_id_(INVALID_TXN_ID),
            prev_lsn_(INVALID_LSN), log_record_type_(LogRecordType::INVALID) {}

  // constructor for Transaction type(BEGIN/COMMIT/ABORT)
  LogRecord(txn_id_t txn_id, lsn_t prev_lsn, LogRecordType log_record_type)
          : size_(HEADER_SIZE), lsn_(INVALID_LSN), txn_id_(txn_id),
            prev_lsn_(prev_lsn), log_record_type_(log_record_type) {}

  // constructor for INSERT/DELETE type
  LogRecord(txn_id_t txn_id, lsn_t prev_lsn, LogRecordType log_record_type,
            const RID &rid, const Tuple &tuple)
          : lsn_(INVALID_LSN), txn_id_(txn_id), prev_lsn_(prev_lsn),
            log_record_type_(log_record_type) {
    if (log_record_type == LogRecordType::INSERT) {
      insert_rid_ = rid;
      insert_tuple_ = tuple;
    } else {
      assert(log_record_type == LogRecordType::APPLYDELETE ||
             log_record_type == LogRecordType::MARKDELETE ||
             log_record_type == LogRecordType::ROLLBACKDELETE);
      delete_rid_ = rid;
      delete_tuple_ = tuple;
    }
    // calculate log record size
    size_ = HEADER_SIZE + sizeof(RID) + sizeof(int32_t) + tuple.GetLength();
  }

  // constructor for UPDATE type
  LogRecord(txn_id_t txn_id, lsn_t prev_lsn, LogRecordType log_record_type,
            const RID &update_rid, const Tuple &old_tuple,
            const Tuple &new_tuple)
          : lsn_(INVALID_LSN), txn_id_(txn_id), prev_lsn_(prev_lsn),
            log_record_type_(log_record_type), update_rid_(update_rid),
            old_tuple_(old_tuple), new_tuple_(new_tuple) {
    // calculate log record size
    size_ = HEADER_SIZE + sizeof(RID) + old_tuple.GetLength() +
            new_tuple.GetLength() + 2 * sizeof(int32_t);
  }

  // constructor for NEWPAGE type
  LogRecord(txn_id_t txn_id, lsn_t prev_lsn, LogRecordType log_record_type,
            page_id_t prev_page_id, page_id_t page_id)
          : size_(HEADER_SIZE), lsn_(INVALID_LSN), txn_id_(txn_id),
            prev_lsn_(prev_lsn), log_record_type_(log_record_type),
            prev_page_id_(prev_page_id), page_id_(page_id) {
    // calculate log record size
    size_ = HEADER_SIZE + sizeof(page_id_t) * 2;
  }

  ~LogRecord() {}

  inline RID &GetDeleteRID() { return delete_rid_; }

  inline Tuple &GetInserteTuple() { return insert_tuple_; }

  inline RID &GetInsertRID() { return insert_rid_; }

  inline page_id_t GetNewPageRecord() { return prev_page_id_; }

  inline int32_t GetSize() { return size_; }

  inline lsn_t GetLSN() { return lsn_; }

  inline txn_id_t GetTxnId() { return txn_id_; }

  inline lsn_t GetPrevLSN() { return prev_lsn_; }

  inline LogRecordType &GetLogRecordType() { return log_record_type_; }

  // For debug purpose
  inline std::string ToString() const {
    std::ostringstream os;
    os << "Log["
       << "size:" << size_ << ", "
       << "LSN:" << lsn_ << ", "
       << "transID:" << txn_id_ << ", "
       << "prevLSN:" << prev_lsn_ << ", "
       << "LogType:" << (int)log_record_type_ << "]";

    return os.str();
  }

private:
  // the length of log record(for serialization, in bytes)
  int32_t size_ = 0;
  // must have fields
  lsn_t lsn_ = INVALID_LSN;
  txn_id_t txn_id_ = INVALID_TXN_ID;
  lsn_t prev_lsn_ = INVALID_LSN;
  LogRecordType log_record_type_ = LogRecordType::INVALID;

  // case1: for delete opeartion, delete_tuple_ for UNDO opeartion
  RID delete_rid_;
  Tuple delete_tuple_;

  // case2: for insert opeartion
  RID insert_rid_;
  Tuple insert_tuple_;

  // case3: for update opeartion
  RID update_rid_;
  Tuple old_tuple_;
  Tuple new_tuple_;

  // case4: for new page opeartion
  page_id_t prev_page_id_ = INVALID_PAGE_ID;
  page_id_t page_id_ = INVALID_PAGE_ID;
  const static int HEADER_SIZE = 20;
}; // namespace cmudb

} // namespace cmudb
