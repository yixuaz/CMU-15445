/**
 * b_plus_tree_test.cpp
 */

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <functional>
#include <iostream>
#include <thread>
#include <random>

#include "buffer/buffer_pool_manager.h"
#include "common/logger.h"
#include "index/b_plus_tree.h"
#include "vtable/virtual_table.h"
#include "gtest/gtest.h"

namespace cmudb {
// helper function to launch multiple threads
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

// helper function to insert
void InsertHelper(BPlusTree<GenericKey<16>, RID, GenericComparator<16>> &tree,
                  const std::vector<int64_t> &keys,
                  __attribute__((unused)) uint64_t thread_itr = 0) {
  GenericKey<16> index_key;
  RID rid;
  // create transaction
  Transaction *transaction = new Transaction(0);
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set((int32_t)(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }
  delete transaction;
}

// helper function to insert and get
void InsertAndGetHelper(BPlusTree<GenericKey<16>, RID, GenericComparator<16>> &tree,
                  const std::vector<int64_t> &keys,
                  __attribute__((unused)) uint64_t thread_itr = 0) {
  GenericKey<16> index_key;
  RID rid;
  // create transaction
  Transaction *transaction = new Transaction(0);
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set((int32_t)(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
    std::vector<RID> rids;
    bool getSuc = tree.GetValue(index_key,rids,transaction);
    EXPECT_EQ(getSuc, true);
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  delete transaction;
}

// helper function to iterate
void IterateHelper(BPlusTree<GenericKey<16>, RID, GenericComparator<16>> &tree) {
  int64_t current_key = 0;
  for (auto iterator = tree.Begin(); iterator.isEnd() == false;
       ++iterator) {
    auto location = (*iterator).second;
    EXPECT_TRUE(location.GetSlotNum()>current_key);
    current_key = location.GetSlotNum();
  }
}

// helper function to seperate insert
void InsertHelperSplit(
    BPlusTree<GenericKey<16>, RID, GenericComparator<16>> &tree,
    const std::vector<int64_t> &keys, int total_threads,
    __attribute__((unused)) uint64_t thread_itr) {
  GenericKey<16> index_key;
  RID rid;
  // create transaction
  Transaction *transaction = new Transaction(0);
  for (auto key : keys) {
    if ((uint64_t)key % total_threads == thread_itr) {
      int64_t value = key & 0xFFFFFFFF;
      rid.Set((int32_t)(key >> 32), value);
      index_key.SetFromInteger(key);
      tree.Insert(index_key, rid, transaction);
    }
  }
  delete transaction;
}

// helper function to delete
void DeleteHelper(BPlusTree<GenericKey<16>, RID, GenericComparator<16>> &tree,
                  const std::vector<int64_t> &remove_keys,
                  __attribute__((unused)) uint64_t thread_itr = 0) {
  GenericKey<16> index_key;
  // create transaction
  Transaction *transaction = new Transaction(0);
  for (auto key : remove_keys) {
    index_key.SetFromInteger(key);
    tree.Remove(index_key, transaction);
  }
  delete transaction;
}

void DeleteAndGetHelper(BPlusTree<GenericKey<16>, RID, GenericComparator<16>> &tree,
                  const std::vector<int64_t> &remove_keys,
                  __attribute__((unused)) uint64_t thread_itr = 0) {
  GenericKey<16> index_key;
  // create transaction
  Transaction *transaction = new Transaction(0);
  for (auto key : remove_keys) {
    index_key.SetFromInteger(key);
    tree.Remove(index_key, transaction);
    std::vector<RID> rids;
    bool getSuc = tree.GetValue(index_key,rids,transaction);
    EXPECT_EQ(getSuc, false);
  }
  delete transaction;
}

// helper function to seperate delete
void DeleteHelperSplit(
    BPlusTree<GenericKey<16>, RID, GenericComparator<16>> &tree,
    const std::vector<int64_t> &remove_keys, int total_threads,
    __attribute__((unused)) uint64_t thread_itr) {
  GenericKey<16> index_key;
  // create transaction
  Transaction *transaction = new Transaction(0);
  for (auto key : remove_keys) {
    if ((uint64_t)key % total_threads == thread_itr) {
      index_key.SetFromInteger(key);
      tree.Remove(index_key, transaction);
    }
  }
  delete transaction;
}

TEST(BPlusTreeConcurrentTest, InsertTest1) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<16> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<16>, RID, GenericComparator<16>> tree("foo_pk", bpm,
                                                           comparator);
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(page_id);
  (void)header_page;
  // keys to Insert
  std::vector<int64_t> keys;
  int64_t scale_factor = 48;
  for (int64_t key = 1; key < scale_factor; key++) {
    keys.push_back(key);
  }
  LaunchParallelTest(4, InsertHelper, std::ref(tree), keys);

  //cout<<tree.ToString()<<endl;
  std::vector<RID> rids;
  GenericKey<16> index_key;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  int64_t start_key = 1;
  int64_t current_key = start_key;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator.isEnd() == false;
       ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }

  EXPECT_EQ(current_key, keys.size() + 1);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  EXPECT_TRUE(tree.Check(true));
  delete key_schema;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeConcurrentTest, InsertTest2) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<16> comparator(key_schema);
  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<16>, RID, GenericComparator<16>> tree("foo_pk", bpm,
                                                           comparator);
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(page_id);
  (void)header_page;
  // keys to Insert
  std::vector<int64_t> keys;
  int64_t scale_factor = 100;
  for (int64_t key = 1; key < scale_factor; key++) {
    keys.push_back(key);
  }
  LaunchParallelTest(2, InsertHelperSplit, std::ref(tree), keys, 2);

  std::vector<RID> rids;
  GenericKey<16> index_key;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  int64_t start_key = 1;
  int64_t current_key = start_key;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator.isEnd() == false;
       ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }

  EXPECT_EQ(current_key, keys.size() + 1);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  EXPECT_TRUE(tree.Check(true));
  delete key_schema;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeConcurrentTest, InsertAndGetTest) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<16> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<16>, RID, GenericComparator<16>> tree("foo_pk", bpm,
                                                             comparator);
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(page_id);
  (void)header_page;
  // keys to Insert
  std::vector<int64_t> keys;
  int64_t scale_factor = 1000;
  for (int64_t key = 1; key < scale_factor; key++) {
    keys.push_back(key);
  }
  LaunchParallelTest(4, InsertAndGetHelper, std::ref(tree), keys);

  //cout<<tree.ToString()<<endl;
  std::vector<RID> rids;
  GenericKey<16> index_key;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  int64_t start_key = 1;
  int64_t current_key = start_key;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator.isEnd() == false;
       ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }

  EXPECT_EQ(current_key, keys.size() + 1);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  EXPECT_TRUE(tree.Check(true));
  delete key_schema;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeConcurrentTest, DeleteTest1) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<16> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<16>, RID, GenericComparator<16>> tree("foo_pk", bpm,
                                                           comparator);
  GenericKey<16> index_key;
  RID rid;
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(page_id);
  (void)header_page;
  // sequential insert
  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  InsertHelper(tree, keys);

  std::vector<int64_t> remove_keys = {1, 5, 3, 4};
  LaunchParallelTest(2, DeleteHelper, std::ref(tree), remove_keys);

  int64_t start_key = 2;
  int64_t current_key = start_key;
  int64_t size = 0;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator.isEnd() == false;
       ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
    size = size + 1;
  }

  EXPECT_EQ(size, 1);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  EXPECT_TRUE(tree.Check(true));
  delete key_schema;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}
TEST(BPlusTreeConcurrentTest, DeleteAndGetTest) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<16> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<16>, RID, GenericComparator<16>> tree("foo_pk", bpm,
                                                             comparator);
  GenericKey<16> index_key;
  RID rid;
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(page_id);
  (void)header_page;
  // sequential insert
  std::vector<int64_t> keys = {1, 2, 3, 4, 5 ,6, 7, 8, 9, 10};
  InsertHelper(tree, keys);

  std::vector<int64_t> remove_keys = {1, 5, 3, 4,6, 7, 8, 9, 10};
  LaunchParallelTest(2, DeleteAndGetHelper, std::ref(tree), remove_keys);

  int64_t start_key = 2;
  int64_t current_key = start_key;
  int64_t size = 0;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator.isEnd() == false;
       ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
    size = size + 1;
  }

  EXPECT_EQ(size, 1);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  EXPECT_TRUE(tree.Check(true));
  delete key_schema;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}
TEST(BPlusTreeConcurrentTest, DeleteTest2) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<16> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<16>, RID, GenericComparator<16>> tree("foo_pk", bpm,
                                                           comparator);
  GenericKey<16> index_key;
  RID rid;
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(page_id);
  (void)header_page;

  // sequential insert
  std::vector<int64_t> keys = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  InsertHelper(tree, keys);

  std::vector<int64_t> remove_keys = {1, 4, 3, 2, 5, 6};
  LaunchParallelTest(2, DeleteHelperSplit, std::ref(tree), remove_keys, 2);

  int64_t start_key = 7;
  int64_t current_key = start_key;
  int64_t size = 0;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator.isEnd() == false;
       ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
    size = size + 1;
  }

  EXPECT_EQ(size, 4);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  EXPECT_TRUE(tree.Check(true));
  delete key_schema;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeConcurrentTest, DeleteTest3) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<16> comparator(key_schema);
  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<16>, RID, GenericComparator<16>> tree("foo_pk", bpm,
                                                           comparator);
  GenericKey<16> index_key;
  RID rid;
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(page_id);
  (void)header_page;
  // sequential insert
  std::vector<int64_t> keys;
  int scale_factor = 100;
  for (int i = 1; i <= scale_factor; i++)
    keys.push_back(i);
  InsertHelper(tree, keys);

  std::vector<int64_t> remove_keys;
  for (int i = 0; i <= (scale_factor - 20); i++)
    remove_keys.push_back(i);
  LaunchParallelTest(2, DeleteHelper, std::ref(tree), remove_keys);

  int64_t start_key = scale_factor - 20 + 1;
  int64_t current_key = start_key;
  int64_t size = 0;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator.isEnd() == false;
       ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
    size = size + 1;
  }

  EXPECT_EQ(size, 20);
  EXPECT_TRUE(tree.Check(true));
  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete disk_manager;
  delete bpm;
  remove("test.db");
}

TEST(BPlusTreeConcurrentTest, DeleteTest4) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<16> comparator(key_schema);
  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<16>, RID, GenericComparator<16>> tree("foo_pk", bpm,
                                                           comparator);
  GenericKey<16> index_key;
  RID rid;
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(page_id);
  (void)header_page;

  // sequential insert
  std::vector<int64_t> keys;
  int scale_factor = 1000;
  for (int i = 1; i <= scale_factor; i++)
    keys.push_back(i);

  InsertHelper(tree, keys);

  std::vector<int64_t> remove_keys;
  for (int i = 1; i <= (scale_factor - 20); i++)
    remove_keys.push_back(i);
  LaunchParallelTest(3, DeleteHelperSplit, std::ref(tree), remove_keys, 3);

  int64_t start_key = scale_factor - 20 + 1;
  int64_t current_key = start_key;
  int64_t size = 0;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator.isEnd() == false;
       ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
    size = size + 1;
  }

  EXPECT_EQ(size, 20);
  EXPECT_TRUE(tree.Check(true));
  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete disk_manager;
  delete bpm;
  remove("test.db");
}

TEST(BPlusTreeConcurrentTest, DeleteTest5) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<16> comparator(key_schema);
  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<16>, RID, GenericComparator<16>> tree("foo_pk", bpm,
                                                           comparator);
  GenericKey<16> index_key;
  RID rid;
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(page_id);
  (void)header_page;
  std::random_device rd;
  std::mt19937 g(rd());

  // sequential insert
  std::vector<int64_t> keys;
  int scale_factor = 1000;
  for (int i = 1; i <= scale_factor; i++)
    keys.push_back(i);
  std::shuffle(begin(keys), end(keys), g);

  InsertHelper(tree, keys);

  std::vector<int64_t> remove_keys;
  for (int i = 1; i <= (scale_factor - 20); i++)
    remove_keys.push_back(i);
  LaunchParallelTest(4, DeleteHelperSplit, std::ref(tree), remove_keys, 4);

  int64_t start_key = scale_factor - 20 + 1;
  int64_t current_key = start_key;
  int64_t size = 0;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator.isEnd() == false;
       ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
    size = size + 1;
  }

  EXPECT_EQ(size, 20);
  EXPECT_TRUE(tree.Check(true));
  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete bpm;
  delete key_schema;
  delete disk_manager;
  remove("test.db");
}


TEST(BPlusTreeConcurrentTest, MixTest) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<16> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<16>, RID, GenericComparator<16>> tree("foo_pk", bpm,
                                                           comparator);
  GenericKey<16> index_key;
  RID rid;

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(page_id);
  (void)header_page;
  // first, populate index
  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  InsertHelper(tree, keys);

  // concurrent insert
  keys.clear();
  for (int i = 6; i <= 10; i++)
    keys.push_back(i);
  LaunchParallelTest(1, InsertHelper, std::ref(tree), keys);
  // concurrent delete
  std::vector<int64_t> remove_keys = {1, 4, 3, 5, 6};
  LaunchParallelTest(1, DeleteHelper, std::ref(tree), remove_keys);

  int64_t start_key = 2;
  int64_t size = 0;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator.isEnd() == false;
       ++iterator) {
    size = size + 1;
  }

  EXPECT_EQ(size, 5);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  EXPECT_TRUE(tree.Check(true));
  delete key_schema;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}
TEST(BPlusTreeConcurrentTest, MixTest2) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<16> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<16>, RID, GenericComparator<16>> tree("foo_pk", bpm,
                                                           comparator);
  GenericKey<16> index_key;
  RID rid;

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(page_id);
  (void) header_page;
  // first, populate index
  std::vector<int64_t> keys, all_deleted;
  for (int i = 1; i <= 1000; ++i) {
    all_deleted.push_back(i);
    keys.push_back(i + 1000);
  }
  // keys1: 0,2,4...
  // keys2: 1,3,5...
  // keys: 100 ~ 200

  // concurrent insert
  LaunchParallelTest(4, InsertHelperSplit, std::ref(tree), std::ref(all_deleted), 4);

  // concurrent insert and delete
  std::thread t0(InsertHelper, std::ref(tree), keys, 0);
  LaunchParallelTest(4, DeleteHelperSplit, std::ref(tree), std::ref(all_deleted), 4);

  t0.join();

  std::vector<RID> rids;
  for (auto key : all_deleted) {
    rids.clear();
    index_key.SetFromInteger(key);
    auto res = tree.GetValue(index_key, rids);
    EXPECT_EQ(false, res);
  }

  int64_t current_key = 1001;
  int64_t size = 0;
  index_key.SetFromInteger(current_key);
  for (auto iterator = tree.Begin(index_key); iterator.isEnd() == false;
       ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
    size = size + 1;
  }

  EXPECT_EQ(size, 1000);
  EXPECT_TRUE(tree.Check(true));
  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}
TEST(BPlusTreeConcurrentTest, MixTest3) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<16> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<16>, RID, GenericComparator<16>> tree("foo_pk", bpm,
                                                             comparator);
  GenericKey<16> index_key;
  RID rid;

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(page_id);
  (void) header_page;
  // first, populate index
  std::vector<int64_t> keys, keys2, deleted1, deleted2;
  int scale = 10000;
  for (int i = 1; i <= scale; ++i) {
    keys.push_back(i);
  }
  std::random_shuffle(keys.begin(), keys.end());
  for (int i = 1; i <= scale/4; ++i) {
    deleted1.push_back(keys.back());
    keys.pop_back();
  }
  for (int i = 1; i <= scale/4; ++i) {
    deleted2.push_back(keys.back());
    keys.pop_back();
  }
  for (int i = 1; i <= scale/4; ++i) {
    keys2.push_back(keys.back());
    keys.pop_back();
  }
//  for (const auto i: keys)
//    cout << i << ' ';
//  cout<<endl;
//  for (const auto i: keys2)
//    cout << i << ' ';
//  cout<<endl;
//  for (const auto i: all_deleted)
//    cout << i << ' ';
//  cout<<endl;


  // concurrent insert
  LaunchParallelTest(4, InsertHelperSplit, std::ref(tree), std::ref(deleted1), 4);
  LaunchParallelTest(4, InsertHelperSplit, std::ref(tree), std::ref(deleted2), 4);
  // concurrent insert and delete
  std::thread t0(InsertAndGetHelper, std::ref(tree), keys, 0);
  std::thread t1(InsertAndGetHelper, std::ref(tree), keys2, 0);
  std::thread t2(DeleteAndGetHelper, std::ref(tree), deleted1, 0);
  std::thread t3(DeleteAndGetHelper, std::ref(tree), deleted2, 0);

  //std::thread t2(IterateHelper, std::ref(tree));
  //LaunchParallelTest(4, DeleteHelperSplit, std::ref(tree), std::ref(all_deleted), 4);

  t0.join();
  t1.join();
  t2.join();
  t3.join();
  EXPECT_TRUE(tree.Check(true));
  std::vector<RID> rids;
  for (auto key : deleted1) {
    rids.clear();
    index_key.SetFromInteger(key);
    auto res = tree.GetValue(index_key, rids);
    EXPECT_EQ(false, res);
  }
  for (auto key : deleted2) {
    rids.clear();
    index_key.SetFromInteger(key);
    auto res = tree.GetValue(index_key, rids);
    EXPECT_EQ(false, res);
  }

  int64_t current_key = 0;
  int64_t size = 0;
  index_key.SetFromInteger(current_key);
  for (auto iterator = tree.Begin(index_key); iterator.isEnd() == false;
       ++iterator) {
    size = size + 1;
  }

  EXPECT_EQ(size, scale/2);
  EXPECT_TRUE(tree.Check(true));
  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

} // namespace cmudb
