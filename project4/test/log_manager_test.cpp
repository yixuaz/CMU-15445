#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <unordered_map>
#include <random>
#include <functional>

#include "logging/common.h"
#include "logging/log_recovery.h"
#include "vtable/virtual_table.h"
#include "table/table_heap.h"
#include "gtest/gtest.h"

namespace cmudb {

TEST(LogManagerTest, BasicLogging) {
  StorageEngine *storage_engine = new StorageEngine("test.db");

  EXPECT_FALSE(ENABLE_LOGGING);
  LOG_DEBUG("Skip system recovering...");

  storage_engine->log_manager_->RunFlushThread();
  EXPECT_TRUE(ENABLE_LOGGING);
  LOG_DEBUG("System logging thread running...");

  LOG_DEBUG("Create a test table");
  Transaction *txn = storage_engine->transaction_manager_->Begin();
  TableHeap *test_table = new TableHeap(storage_engine->buffer_pool_manager_,
                                        storage_engine->lock_manager_,
                                        storage_engine->log_manager_, txn);
  LOG_DEBUG("Insert and delete a random tuple");

  std::string createStmt =
          "a varchar, b smallint, c bigint, d bool, e varchar(16)";
  Schema *schema = ParseCreateStatement(createStmt);
  RID rid;
  Tuple tuple = ConstructTuple(schema);
  EXPECT_TRUE(test_table->InsertTuple(tuple, rid, txn));
  EXPECT_TRUE(test_table->MarkDelete(rid, txn));
  storage_engine->transaction_manager_->Commit(txn);
  LOG_DEBUG("Commit txn");

  storage_engine->log_manager_->StopFlushThread();
  EXPECT_FALSE(ENABLE_LOGGING);
  LOG_DEBUG("Turning off flushing thread");

  // some basic manually checking here
  char buffer[PAGE_SIZE];
  storage_engine->disk_manager_->ReadLog(buffer, PAGE_SIZE, 0);
  int32_t size = *reinterpret_cast<int32_t *>(buffer);
  LOG_DEBUG("size  = %d", size);
  size = *reinterpret_cast<int32_t *>(buffer + 20);
  LOG_DEBUG("size  = %d", size);
  size = *reinterpret_cast<int32_t *>(buffer + 48);
  LOG_DEBUG("size  = %d", size);

  delete txn;
  delete storage_engine;
  delete test_table;
  delete schema;
  LOG_DEBUG("Teared down the system");
  remove("test.db");
  remove("test.log");
}
void StartTransaction(StorageEngine* storage_engine, TableHeap* test_table)
{
  LOG_DEBUG("Create a test table");
  Transaction *txn = storage_engine->transaction_manager_->Begin();
  LOG_DEBUG("Insert and delete a random tuple");

  std::string createStmt =
          "a varchar, b smallint, c bigint, d bool, e varchar(16)";
  Schema *schema = ParseCreateStatement(createStmt);
  RID rid;
  Tuple tuple = ConstructTuple(schema);
  EXPECT_TRUE(test_table->InsertTuple(tuple, rid, txn));
  EXPECT_TRUE(test_table->MarkDelete(rid, txn));
  LOG_DEBUG("Commit txn %d", txn->GetTransactionId());
  storage_engine->transaction_manager_->Commit(txn);
  delete txn;
}


void StartTransaction1(StorageEngine* storage_engine, TableHeap* test_table)
{
  LOG_DEBUG("Create a test table");
  Transaction *txn = storage_engine->transaction_manager_->Begin();
  LOG_DEBUG("Insert and delete a random tuple");

  for (int i = 0; i < 10; i++)
  {
    std::string createStmt =
            "a varchar, b smallint, c bigint, d bool, e varchar(16)";
    Schema *schema = ParseCreateStatement(createStmt);
    RID rid;
    Tuple tuple = ConstructTuple(schema);
    EXPECT_TRUE(test_table->InsertTuple(tuple, rid, txn));
  }
  LOG_DEBUG("Commit txn %d", txn->GetTransactionId());
  storage_engine->transaction_manager_->Commit(txn);
  delete txn;
}


TEST(LogManagerTest, LoggingWithGroupCommit) {
  StorageEngine *storage_engine = new StorageEngine("test.db");
  EXPECT_FALSE(ENABLE_LOGGING);
  LOG_DEBUG("Skip system recovering...");

  storage_engine->log_manager_->RunFlushThread();
  EXPECT_TRUE(ENABLE_LOGGING);
  LOG_DEBUG("System logging thread running...");

  LOG_DEBUG("Create a test table");
  Transaction *txn = storage_engine->transaction_manager_->Begin();
  TableHeap *test_table = new TableHeap(storage_engine->buffer_pool_manager_,
                                        storage_engine->lock_manager_,
                                        storage_engine->log_manager_, txn);
  LOG_DEBUG("Insert and delete a random tuple");

  std::string createStmt =
          "a varchar, b smallint, c bigint, d bool, e varchar(16)";
  Schema *schema = ParseCreateStatement(createStmt);
  RID rid;
  Tuple tuple = ConstructTuple(schema);
  EXPECT_TRUE(test_table->InsertTuple(tuple, rid, txn));
  EXPECT_TRUE(test_table->MarkDelete(rid, txn));
  LOG_DEBUG("Commit txn %d", txn->GetTransactionId());
  storage_engine->transaction_manager_->Commit(txn);
  delete txn;



  std::future<void> fut1 = std::async(std::launch::async, StartTransaction, storage_engine, test_table);
  std::future<void> fut2 = std::async(std::launch::async, StartTransaction, storage_engine, test_table);
  std::future<void> fut3 = std::async(std::launch::async, StartTransaction, storage_engine, test_table);


  fut1.get();
  fut2.get();
  fut3.get();


  storage_engine->log_manager_->StopFlushThread();
  EXPECT_FALSE(ENABLE_LOGGING);
  LOG_DEBUG("Turning off flushing thread");

  // some basic manually checking here
  char buffer[PAGE_SIZE];
  storage_engine->disk_manager_->ReadLog(buffer, PAGE_SIZE, 0);
  int32_t size = *reinterpret_cast<int32_t *>(buffer);
  LOG_DEBUG("size  = %d", size);
  size = *reinterpret_cast<int32_t *>(buffer + 20);
  LOG_DEBUG("size  = %d", size);
  size = *reinterpret_cast<int32_t *>(buffer + 48);
  LOG_DEBUG("size  = %d", size);

  delete storage_engine;
  delete test_table;
  delete schema;
  LOG_DEBUG("Teared down the system");
  remove("test.db");
  remove("test.log");
}


TEST(LogManagerTest, SingleLoggingWithBufferFull) {

  StorageEngine *storage_engine = new StorageEngine("test.db");
  EXPECT_FALSE(ENABLE_LOGGING);
  LOG_DEBUG("Skip system recovering...");

  storage_engine->log_manager_->RunFlushThread();
  EXPECT_TRUE(ENABLE_LOGGING);
  LOG_DEBUG("System logging thread running...");

  LOG_DEBUG("Create a test table");
  Transaction *txn = storage_engine->transaction_manager_->Begin();
  TableHeap *test_table = new TableHeap(storage_engine->buffer_pool_manager_,
                                        storage_engine->lock_manager_,
                                        storage_engine->log_manager_, txn);
  LOG_DEBUG("Insert and delete a random tuple");

  for (int i = 0; i < 13; i++)
  {
    std::string createStmt =
            "a varchar, b smallint, c bigint, d bool, e varchar(16)";
    Schema *schema = ParseCreateStatement(createStmt);
    RID rid;
    Tuple tuple = ConstructTuple(schema);
    EXPECT_TRUE(test_table->InsertTuple(tuple, rid, txn));
  }
  LOG_DEBUG("Commit txn %d", txn->GetTransactionId());
  storage_engine->transaction_manager_->Commit(txn);
  delete txn;

  storage_engine->log_manager_->StopFlushThread();
  EXPECT_FALSE(ENABLE_LOGGING);
  LOG_DEBUG("Turning off flushing thread");
  LOG_DEBUG("num of flushes = %d", storage_engine->disk_manager_->GetNumFlushes());

  // some basic manually checking here
  char buffer[PAGE_SIZE];
  storage_engine->disk_manager_->ReadLog(buffer, PAGE_SIZE, 0);
  int32_t size = *reinterpret_cast<int32_t *>(buffer);
  LOG_DEBUG("size  = %d", size);
  size = *reinterpret_cast<int32_t *>(buffer + 20);
  LOG_DEBUG("size  = %d", size);
  size = *reinterpret_cast<int32_t *>(buffer + 48);
  LOG_DEBUG("size  = %d", size);

  delete storage_engine;
  LOG_DEBUG("Teared down the system");
  remove("test.db");
  remove("test.log");
}

TEST(LogManagerTest, MultiLoggingWithBufferFull) {

  StorageEngine *storage_engine = new StorageEngine("test.db");
  EXPECT_FALSE(ENABLE_LOGGING);
  LOG_DEBUG("Skip system recovering...");

  storage_engine->log_manager_->RunFlushThread();
  EXPECT_TRUE(ENABLE_LOGGING);
  LOG_DEBUG("System logging thread running...");

  LOG_DEBUG("Create a test table");
  Transaction *txn = storage_engine->transaction_manager_->Begin();
  TableHeap *test_table = new TableHeap(storage_engine->buffer_pool_manager_,
                                        storage_engine->lock_manager_,
                                        storage_engine->log_manager_, txn);
  LOG_DEBUG("Insert and delete a random tuple");

  for (int i = 0; i < 13; i++)
  {
    std::string createStmt =
            "a varchar, b smallint, c bigint, d bool, e varchar(16)";
    Schema *schema = ParseCreateStatement(createStmt);
    RID rid;
    Tuple tuple = ConstructTuple(schema);
    EXPECT_TRUE(test_table->InsertTuple(tuple, rid, txn));
  }
  LOG_DEBUG("Commit txn %d", txn->GetTransactionId());
  storage_engine->transaction_manager_->Commit(txn);
  delete txn;

#if 1
  std::future<void> fut1 = std::async(std::launch::async, StartTransaction1, storage_engine, test_table);
  std::future<void> fut2 = std::async(std::launch::async, StartTransaction1, storage_engine, test_table);


  fut1.get();
  fut2.get();
#endif
  storage_engine->log_manager_->StopFlushThread();
  EXPECT_FALSE(ENABLE_LOGGING);
  LOG_DEBUG("Turning off flushing thread");


  LOG_DEBUG("num of flushes = %d", storage_engine->disk_manager_->GetNumFlushes());
  // some basic manually checking here
  char buffer[PAGE_SIZE];
  storage_engine->disk_manager_->ReadLog(buffer, PAGE_SIZE, 0);
  int32_t size = *reinterpret_cast<int32_t *>(buffer);
  LOG_DEBUG("size  = %d", size);
  size = *reinterpret_cast<int32_t *>(buffer + 20);
  LOG_DEBUG("size  = %d", size);
  size = *reinterpret_cast<int32_t *>(buffer + 48);
  LOG_DEBUG("size  = %d", size);

  delete storage_engine;
  LOG_DEBUG("Teared down the system");
  remove("test.db");
  remove("test.log");
}
// actually LogRecovery
TEST(LogManagerTest, RedoTestWithOneTxn) {
  StorageEngine *storage_engine = new StorageEngine("test.db");

  EXPECT_FALSE(ENABLE_LOGGING);
  LOG_DEBUG("Skip system recovering...");

  storage_engine->log_manager_->RunFlushThread();
  EXPECT_TRUE(ENABLE_LOGGING);
  LOG_DEBUG("System logging thread running...");

  LOG_DEBUG("Create a test table");
  Transaction *txn = storage_engine->transaction_manager_->Begin();
  TableHeap *test_table = new TableHeap(storage_engine->buffer_pool_manager_,
                                        storage_engine->lock_manager_,
                                        storage_engine->log_manager_, txn);
  page_id_t first_page_id = test_table->GetFirstPageId();

  std::string createStmt =
          "a varchar, b smallint, c bigint, d bool, e varchar(16)";
  Schema *schema = ParseCreateStatement(createStmt);

  RID rid;
  Tuple tuple = ConstructTuple(schema);
  std::cout << "Tuple: " << tuple.ToString(schema) << "\n";
  Tuple tuple1 = ConstructTuple(schema);
  std::cout << "Tuple1: " << tuple1.ToString(schema) << "\n";

  auto val = tuple.GetValue(schema, 4);
  EXPECT_TRUE(test_table->InsertTuple(tuple, rid, txn));
  storage_engine->transaction_manager_->Commit(txn);
  delete txn;
  delete test_table;
  LOG_DEBUG("Commit txn");

  LOG_DEBUG("SLEEPING for 2s");
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  // shutdown System
  delete storage_engine;
  LOG_DEBUG("AFTER delete");
  // restart system
  storage_engine = new StorageEngine("test.db");
  LOG_DEBUG("BEFORE LogRecovery");
  LogRecovery *log_recovery = new LogRecovery(
          storage_engine->disk_manager_, storage_engine->buffer_pool_manager_);
  LOG_DEBUG("BEFORE REDO");
  log_recovery->Redo();
  LOG_DEBUG("REDO DONE");
  log_recovery->Undo();

  Tuple old_tuple;
  txn = storage_engine->transaction_manager_->Begin();
  test_table = new TableHeap(storage_engine->buffer_pool_manager_,
                             storage_engine->lock_manager_,
                             storage_engine->log_manager_, first_page_id);
  EXPECT_EQ(test_table->GetTuple(rid, old_tuple, txn), 1);
  storage_engine->transaction_manager_->Commit(txn);
  delete txn;
  delete test_table;

  EXPECT_EQ(old_tuple.GetValue(schema, 4).CompareEquals(val), 1);

  delete storage_engine;
  LOG_DEBUG("Teared down the system");
  remove("test.db");
  remove("test.log");
}

TEST(LogManagerTest, RedoInsertTest) {
  StorageEngine *storage_engine = new StorageEngine("test.db");

  EXPECT_FALSE(ENABLE_LOGGING);
  LOG_DEBUG("Skip system recovering...");

  storage_engine->log_manager_->RunFlushThread();
  EXPECT_TRUE(ENABLE_LOGGING);
  LOG_DEBUG("System logging thread running...");

  LOG_DEBUG("Create a test table");
  Transaction *txn = storage_engine->transaction_manager_->Begin();
  TableHeap *test_table = new TableHeap(storage_engine->buffer_pool_manager_,
                                        storage_engine->lock_manager_,
                                        storage_engine->log_manager_, txn);
  page_id_t first_page_id = test_table->GetFirstPageId();

  std::string createStmt =
          "a varchar, b smallint, c bigint, d bool, e varchar(16)";
  Schema *schema = ParseCreateStatement(createStmt);

  RID rid;
  Tuple tuple = ConstructTuple(schema);
  std::cout << "Tuple: " << tuple.ToString(schema) << "\n";

  auto val = tuple.GetValue(schema, 4);
  EXPECT_TRUE(test_table->InsertTuple(tuple, rid, txn));
  delete txn;
  delete test_table;
  LOG_DEBUG("Crash before Commit txn...");

  LOG_DEBUG("SLEEPING for 2s");
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  // shutdown System
  delete storage_engine;

  // restart system
  storage_engine = new StorageEngine("test.db");

  LogRecovery *log_recovery = new LogRecovery(storage_engine->disk_manager_, storage_engine->buffer_pool_manager_);

  log_recovery->Redo();
  log_recovery->Undo();

  Tuple old_tuple;
  txn = storage_engine->transaction_manager_->Begin();
  test_table = new TableHeap(storage_engine->buffer_pool_manager_,
                             storage_engine->lock_manager_,
                             storage_engine->log_manager_, first_page_id);
  // tuple insertion is undo. So no such tuple.
  EXPECT_EQ(test_table->GetTuple(rid, old_tuple, txn), 0);
  storage_engine->transaction_manager_->Commit(txn);

  delete txn;
  delete test_table;

  delete storage_engine;
  LOG_DEBUG("Teared down the system");
  remove("test.db");
  remove("test.log");
}

TEST(LogManagerTest, RedoDeleteTest) {
  remove("test.db");
  remove("test.log");
  // commit txn1 for insert. Tx1 for delete crash before commit
  // expected result is tuple exists.
  StorageEngine *storage_engine = new StorageEngine("test.db");

  EXPECT_FALSE(ENABLE_LOGGING);
  LOG_DEBUG("Skip system recovering...");

  storage_engine->log_manager_->RunFlushThread();
  EXPECT_TRUE(ENABLE_LOGGING);
  LOG_DEBUG("System logging thread running...");

  LOG_DEBUG("Create a test table");
  Transaction *txn = storage_engine->transaction_manager_->Begin();
  TableHeap *test_table = new TableHeap(storage_engine->buffer_pool_manager_,
                                        storage_engine->lock_manager_,
                                        storage_engine->log_manager_, txn);
  page_id_t first_page_id = test_table->GetFirstPageId();

  std::string createStmt =
          "a varchar, b smallint, c bigint, d bool, e varchar(16)";
  Schema *schema = ParseCreateStatement(createStmt);

  RID rid;
  Tuple tuple = ConstructTuple(schema);
  std::cout << "Tuple: " << tuple.ToString(schema) << "\n";

  auto val = tuple.GetValue(schema, 4);
  EXPECT_TRUE(test_table->InsertTuple(tuple, rid, txn));
  storage_engine->transaction_manager_->Commit(txn);
  LOG_DEBUG("Commit txn...");

// txn 2
  Transaction *txn2 = storage_engine->transaction_manager_->Begin();
  LOG_DEBUG("delete ...............................");
  EXPECT_TRUE(test_table->MarkDelete(rid, txn2));
  test_table->ApplyDelete(rid, txn2);

  LOG_DEBUG("SLEEPING for 1s");
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  delete txn;
  delete txn2;
  delete test_table;
  LOG_DEBUG("Crash before Commit txn2...");

  LOG_DEBUG("SLEEPING for 2s");
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  // shutdown System
  delete storage_engine;

  // restart system
  storage_engine = new StorageEngine("test.db");

  LogRecovery *log_recovery = new LogRecovery(storage_engine->disk_manager_, storage_engine->buffer_pool_manager_);

  log_recovery->Redo();
  log_recovery->Undo();

  Tuple old_tuple;
  txn = storage_engine->transaction_manager_->Begin();
  test_table = new TableHeap(storage_engine->buffer_pool_manager_,
                             storage_engine->lock_manager_,
                             storage_engine->log_manager_, first_page_id);
  // tuple insertion is undo. So no such tuple.
  EXPECT_EQ(test_table->GetTuple(rid, old_tuple, txn), 1);
  storage_engine->transaction_manager_->Commit(txn);
  std::cout << "Old Tuple: " << old_tuple.ToString(schema) << "\n";

  delete txn;
  delete test_table;

  delete storage_engine;
  LOG_DEBUG("Teared down the system");
  remove("test.db");
  remove("test.log");
}
//// helper function to launch multiple threads
template <typename... Args>
void LaunchParallelTest(uint64_t num_threads, Args &&... args) {
  std::vector<std::thread> thread_group;

  // Launch a group of threads
  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group.push_back(std::thread(args..., thread_itr));
  }

  // Join the threads with the main thread
  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group[thread_itr].join();
  }
}

static std::random_device rd;
static std::mt19937 mt(rd());
static std::uniform_int_distribution<int> dis(0, 2);

void RandomOp(StorageEngine *storage_engine, TableHeap *table, Schema *schema,
              std::unordered_map<RID, Tuple> *vals) {
  auto *txn = storage_engine->transaction_manager_->Begin();
  RID rid;
  Tuple tuple = ConstructTuple(schema);
  EXPECT_TRUE(table->InsertTuple(tuple, rid, txn));
  switch (dis(mt)) {
    case 0:
      (*vals)[rid] = tuple;
      break;

    case 1: {
      Tuple new_tuple = ConstructTuple(schema);
      (*vals)[rid] = table->UpdateTuple(new_tuple, rid, txn) ? new_tuple : tuple;
      break;
    }

    case 2:
      table->MarkDelete(rid, txn);
      break;
  }
  if (dis(mt) == 2) {
    storage_engine->transaction_manager_->Abort(txn);
    vals->erase(rid);
  } else {
    storage_engine->transaction_manager_->Commit(txn);
  }
  delete txn;
}

void RandomOpHelper(StorageEngine *storage_engine, TableHeap *table, Schema *schema,
                    int ops, std::unordered_map<RID, Tuple> *maps, uint64_t thread_itr) {
  auto &vals = maps[thread_itr];
  for (int i = 0; i < ops; i++) {
    RandomOp(storage_engine, table, schema, &vals);
  }
}

TEST(LogManagerTest, StressTest) {
  remove("test.db");
  remove("test.log");
  StorageEngine *storage_engine = new StorageEngine("test.db");

  EXPECT_FALSE(ENABLE_LOGGING);
  LOG_DEBUG("Skip system recovering...");

  storage_engine->log_manager_->RunFlushThread();
  EXPECT_TRUE(ENABLE_LOGGING);
  LOG_DEBUG("System logging thread running...");

  LOG_DEBUG("Create a test table");
  Transaction *txn = storage_engine->transaction_manager_->Begin();
  TableHeap *test_table = new TableHeap(storage_engine->buffer_pool_manager_,
                                        storage_engine->lock_manager_,
                                        storage_engine->log_manager_, txn);
  storage_engine->transaction_manager_->Commit(txn);
  delete txn;
  page_id_t first_page_id = test_table->GetFirstPageId();

  std::string createStmt =
          "a varchar, b smallint, c bigint, d bool, e varchar(16)";
  Schema *schema = ParseCreateStatement(createStmt);

  std::unordered_map<RID, Tuple> maps[5];
  LaunchParallelTest(5, RandomOpHelper, storage_engine, test_table, schema, 100, maps);

  for (size_t i = 1; i < sizeof(maps) / sizeof(maps[0]); i++)
    maps[0].insert(maps[i].begin(), maps[i].end());
  auto &vals = maps[0];

  int size = 0;
  txn = storage_engine->transaction_manager_->Begin();
  for (auto iter = test_table->begin(txn), end = test_table->end(); iter != end; ++iter, ++size) {
    auto found = vals.find(iter->GetRid());
    EXPECT_TRUE(found != vals.end());
    EXPECT_EQ(1, found->second.GetValue(schema, 4).CompareEquals(iter->GetValue(schema, 4)));
  }
  EXPECT_EQ(vals.size(), size);
  storage_engine->transaction_manager_->Commit(txn);
  delete txn;
  delete test_table;

  LOG_DEBUG("SLEEPING for 2s");
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  LOG_DEBUG("Shutdown system");
  delete storage_engine;

  // restart system
  LOG_DEBUG("Restart system");
  storage_engine = new StorageEngine("test.db");
  LogRecovery *log_recovery = new LogRecovery(
          storage_engine->disk_manager_, storage_engine->buffer_pool_manager_);

  log_recovery->Redo();
  log_recovery->Undo();
  LOG_DEBUG("System recovering done");

  test_table = new TableHeap(storage_engine->buffer_pool_manager_,
                             storage_engine->lock_manager_,
                             storage_engine->log_manager_, first_page_id);
  txn = storage_engine->transaction_manager_->Begin();
  size = 0;
  for (auto iter = test_table->begin(txn), end = test_table->end(); iter != end; ++iter, ++size) {
    auto found = vals.find(iter->GetRid());
    EXPECT_TRUE(found != vals.end());
    EXPECT_EQ(1, found->second.GetValue(schema, 4).CompareEquals(iter->GetValue(schema, 4)));
  }
  EXPECT_EQ(vals.size(), size);
  storage_engine->transaction_manager_->Commit(txn);
  delete txn;

  delete test_table;
  delete storage_engine;
  delete log_recovery;
  delete schema;
  LOG_DEBUG("Teared down the system");
  remove("test.db");
  remove("test.log");
}

TEST(LogManagerTest, UndoTest) {
  StorageEngine *storage_engine = new StorageEngine("test.db");

  EXPECT_FALSE(ENABLE_LOGGING);
  LOG_DEBUG("Skip system recovering...");

  storage_engine->log_manager_->RunFlushThread();
  EXPECT_TRUE(ENABLE_LOGGING);
  LOG_DEBUG("System logging thread running...");

  LOG_DEBUG("Create a test table");
  Transaction *txn = storage_engine->transaction_manager_->Begin();
  TableHeap *test_table = new TableHeap(storage_engine->buffer_pool_manager_,
                                        storage_engine->lock_manager_,
                                        storage_engine->log_manager_, txn);
  page_id_t first_page_id = test_table->GetFirstPageId();

  LOG_DEBUG("Insert a random tuple");
  std::string createStmt =
          "a varchar, b smallint, c bigint, d bool, e varchar(16)";
  Schema *schema = ParseCreateStatement(createStmt);
  RID rid;
  Tuple tuple = ConstructTuple(schema);
  EXPECT_TRUE(test_table->InsertTuple(tuple, rid, txn));
  storage_engine->transaction_manager_->Commit(txn);
  delete txn;

  auto undo_test = [&](std::function<void()> func, const std::string name) {
    LOG_DEBUG("Undo %s test", name.c_str());
    func();

    delete test_table;
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    LOG_DEBUG("AFTER SLEEP");
    delete storage_engine;
    LOG_DEBUG("AFTER delete");
    // restart system
    storage_engine = new StorageEngine("test.db");
    LOG_DEBUG("NEW ENGINE");
    LogRecovery *log_recovery = new LogRecovery(
            storage_engine->disk_manager_, storage_engine->buffer_pool_manager_);
    LOG_DEBUG("START Redo %s", name.c_str());
    log_recovery->Redo();
    LOG_DEBUG("START Undo %s", name.c_str());
    log_recovery->Undo();

    test_table = new TableHeap(storage_engine->buffer_pool_manager_,
                               storage_engine->lock_manager_,
                               storage_engine->log_manager_, first_page_id);
    txn = storage_engine->transaction_manager_->Begin();
    int size = 0;
    for (auto iter = test_table->begin(txn), end = test_table->end(); iter != end; ++iter) {
      EXPECT_EQ(1, tuple.GetValue(schema, 4).CompareEquals(iter->GetValue(schema, 4)));
      size++;
    }
    EXPECT_EQ(1, size);
    storage_engine->transaction_manager_->Commit(txn);
    delete txn;
    delete log_recovery;
  };

  undo_test([&] {
    auto *txn = storage_engine->transaction_manager_->Begin();
    LOG_DEBUG("TXN BEG");
    auto tuple = ConstructTuple(schema);
    RID new_rid;
    LOG_DEBUG("BEFORE INST");
    EXPECT_TRUE(test_table->InsertTuple(tuple, new_rid, txn));
    LOG_DEBUG("AFTER INST");
    delete txn;
  }, "Insert");

  undo_test([&] {
    auto *txn = storage_engine->transaction_manager_->Begin();
    auto tuple = ConstructTuple(schema);
    EXPECT_TRUE(test_table->UpdateTuple(tuple, rid, txn));
    delete txn;
  }, "Update");

  undo_test([&] {
    auto *txn = storage_engine->transaction_manager_->Begin();
    EXPECT_TRUE(test_table->MarkDelete(rid, txn));
    delete txn;
  }, "MarkDelete");

  undo_test([&] {
    auto *txn = storage_engine->transaction_manager_->Begin();
    EXPECT_TRUE(test_table->MarkDelete(rid, txn));
    test_table->ApplyDelete(rid, txn);
    delete txn;
  }, "ApplyDelete");

  undo_test([&] {
    auto *txn = storage_engine->transaction_manager_->Begin();
    EXPECT_TRUE(test_table->MarkDelete(rid, txn));
    test_table->RollbackDelete(rid, txn);
    delete txn;
  }, "RollbackDelete");

  delete test_table;
  delete storage_engine;
  delete schema;

  LOG_DEBUG("Teared down the system");
  remove("test.db");
  remove("test.log");
}

} // namespace cmudb
