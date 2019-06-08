/**
 * b_plus_tree_test.cpp
 */

#include <algorithm>
#include <cstdio>
#include <iostream>
#include <sstream>

#include "buffer/buffer_pool_manager.h"
#include "common/logger.h"
#include "index/b_plus_tree.h"
#include "vtable/virtual_table.h"
#include "gtest/gtest.h"

namespace cmudb {

TEST(BPlusTreeDeleteTests, DeleteTest1) {
  // create KeyComparator and index schema
  std::string createStmt = "a bigint";
  Schema *key_schema = ParseCreateStatement(createStmt);
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm,
                                                           comparator);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  Transaction *transaction = new Transaction(0);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(page_id);
  (void)header_page;

  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set((int32_t)(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }

  ASSERT_TRUE(bpm->CheckAllUnpined());
  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  ASSERT_TRUE(bpm->CheckAllUnpined());
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
  ASSERT_TRUE(bpm->CheckAllUnpined());
  EXPECT_EQ(current_key, keys.size() + 1);

  std::vector<int64_t> remove_keys = {1, 5};
  for (auto key : remove_keys) {
    index_key.SetFromInteger(key);
    cout<<key<<endl;
    tree.Remove(index_key, transaction);
  }
  ASSERT_TRUE(bpm->CheckAllUnpined());
  start_key = 2;
  current_key = start_key;
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

  EXPECT_EQ(size, 3);
  ASSERT_TRUE(bpm->CheckAllUnpined());
  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete transaction;
  delete disk_manager;
  delete bpm;
  delete key_schema;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeDeleteTests, DeleteTest2) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm,
                                                           comparator);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  Transaction *transaction = new Transaction(0);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(page_id);
  (void)header_page;

  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set((int32_t)(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }

  std::vector<RID> rids;
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

  std::vector<int64_t> remove_keys = {1, 5, 3, 4};
  for (auto key : remove_keys) {
    index_key.SetFromInteger(key);
    tree.Remove(index_key, transaction);
  }

  start_key = 2;
  current_key = start_key;
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
  ASSERT_TRUE(bpm->CheckAllUnpined());
  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete transaction;
  delete disk_manager;
  delete bpm;
  delete key_schema;
  remove("test.db");
  remove("test.log");
}
TEST(BPlusTreeDeleteTests, DeleteBasic) {
  // create KeyComparator and index schema
  std::string createStmt = "a bigint";
  Schema *key_schema = ParseCreateStatement(createStmt);
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm,
                                                           comparator);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  Transaction *transaction = new Transaction(0);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(page_id);
  (void) header_page;

  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set((int32_t) (key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }

  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  int64_t start_key = keys[0];
  int64_t current_key = start_key;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator.isEnd() == false;
       ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    ++current_key;
  }

  EXPECT_EQ(current_key, keys.size() + start_key);

  std::vector<int64_t> remove_keys = {2, 5, 3, 1,4};
  for (auto key : remove_keys) {
    index_key.SetFromInteger(key);
    tree.Remove(index_key, transaction);
  }

  // all gone
  rids.clear();
  for (auto key : remove_keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, rids);
    EXPECT_EQ(rids.size(), 0);
  }
  ASSERT_TRUE(bpm->CheckAllUnpined());
  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete transaction;
  delete disk_manager;
  delete bpm;
  delete key_schema;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeDeleteTests, DeleteScale) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<16> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(9, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<16>, RID, GenericComparator<16>> tree("foo_pk", bpm,
                                                             comparator);
  GenericKey<16> index_key;
  RID rid;
  // create transaction
  Transaction *transaction = new Transaction(0);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(page_id);
  (void) header_page;

  int scale = 15;
  std::vector<int64_t> keys;// = {2,6,9,3,1,5,14,13,10,4,7,15,12,8,11};
  for (int i = 0; i < scale; ++i) {
    keys.push_back(i + 1);
  }
//  std::random_shuffle(keys.begin(), keys.end());
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set((int32_t) (key >> 32), value);
    index_key.SetFromInteger(key);
    //std::cout<<"insert :"<<key<<endl;
    tree.Insert(index_key, rid, transaction);
    std::cout<<tree.ToString()<<endl;
  }
  // check all value is in the tree
  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  // range query
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
  ASSERT_TRUE(tree.Check(true));

  // delete all
  for (auto key :keys) {
    index_key.SetFromInteger(key);
    std::cout<<"remove :"<<index_key<<endl;
    tree.Remove(index_key, transaction);
    std::cout<<tree.ToString()<<endl;
  }
  ASSERT_TRUE(tree.Check(true));
  // check all value is in the tree
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, rids);
    EXPECT_EQ(rids.size(), 0);
  }
  
  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete transaction;
  delete disk_manager;
  delete bpm;
  delete key_schema;
  remove("test.db");
  remove("test.log");
}
TEST(BPlusTreeDeleteTests, DeleteRandom) {
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
  // create transaction
  Transaction *transaction = new Transaction(0);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(page_id);
  (void) header_page;

  tree.openCheck = false;
  std::vector<int64_t> keys;
  int scale = 10000;
  for (int i = 0; i < scale; ++i) {
    keys.push_back(i + 1);
  }
  std::random_shuffle(keys.begin(), keys.end());

  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set((int32_t) (key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }
  ASSERT_TRUE(tree.Check(true));
  std::random_shuffle(keys.begin(), keys.end());
  // delete all
  for (auto key :keys) {
    index_key.SetFromInteger(key);
    tree.Remove(index_key, transaction);
  }
  ASSERT_TRUE(tree.Check(true));
  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete transaction;
  delete disk_manager;
  delete bpm;
  delete key_schema;
  remove("test.db");
  remove("test.log");
}

} // namespace cmudb
