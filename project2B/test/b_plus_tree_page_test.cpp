/**
 * b_plus_tree_page_test.cpp
 */

#include <cstdio>

#include "gtest/gtest.h"
#include "buffer/buffer_pool_manager.h"
#include "common/logger.h"
#include "common/config.h"
#include "page/b_plus_tree_leaf_page.h"
#include "vtable/virtual_table.h"


namespace cmudb {

void setKeyValue(int64_t k, GenericKey<8> &index_key, RID &rid) {
  index_key.SetFromInteger(k);
  int64_t value = k & 0xFFFFFFFF;
  rid.Set((int32_t)(k >> 32), value);
}
TEST(BPlusTreePageTests, testInternalPage) {
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);


  GenericKey<8> index_key;

  page_id_t root_page_id;
  Page *root_page = bpm->NewPage(root_page_id);
  page_id_t p0, p1, p2, p3, p4;
  bpm->NewPage(p0);
  bpm->NewPage(p1);
  bpm->NewPage(p2);
  bpm->NewPage(p3);
  bpm->NewPage(p4);

  BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>> *ip = reinterpret_cast<BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>> *>(root_page->GetData());
  ip->Init(root_page_id);
  ip->SetMaxSize(4);
  index_key.SetFromInteger(1);
  ip->PopulateNewRoot(p0, index_key, p1);
  EXPECT_EQ(2, ip->GetSize());
  EXPECT_EQ(p0, ip->ValueAt(0));
  EXPECT_EQ(p1, ip->ValueAt(1));

  // 当前数据:[<invalid, p0>, <1, p1>]，测试InsertNodeAfter()
  index_key.SetFromInteger(3);
  ip->InsertNodeAfter(p1, index_key, p3);
  index_key.SetFromInteger(2);
  ip->InsertNodeAfter(p1, index_key, p2);
  EXPECT_EQ(4, ip->GetSize());
  EXPECT_EQ(p0, ip->ValueAt(0));
  EXPECT_EQ(p1, ip->ValueAt(1));
  EXPECT_EQ(p2, ip->ValueAt(2));
  EXPECT_EQ(p3, ip->ValueAt(3));
  // 当前数据:[<invalid, p0>, <1, p1>, <2, p2>, <3, p3>]

  // 测试Lookup()
  index_key.SetFromInteger(0);
  EXPECT_EQ(p0, ip->Lookup(index_key, comparator));
  index_key.SetFromInteger(1);
  EXPECT_EQ(p1, ip->Lookup(index_key, comparator));
  index_key.SetFromInteger(20);
  EXPECT_EQ(p3, ip->Lookup(index_key, comparator));

  // 测试MoveHalfTo()，分裂后ip指向的数据:[<invalid, p0>, <1, p1>]
  // new_ip指向的数据:[<2, p2>,<3, p3>, <4, p4>]
  index_key.SetFromInteger(4);
  ip->InsertNodeAfter(p3, index_key, p4);
  page_id_t new_page_id;
  Page *new_page = bpm->NewPage(new_page_id);
  BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>> *new_ip = reinterpret_cast<BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>> *>(new_page->GetData());
  ip->MoveHalfTo(new_ip, bpm);
  EXPECT_EQ(2, ip->GetSize());
  EXPECT_EQ(3, new_ip->GetSize());
  index_key.SetFromInteger(3);
  //EXPECT_EQ(index_key, new_ip->KeyAt(0));

  // 测试Remove()，删除后ip指向的数据:[<invalid, p0>, <2, p2>]
  ip->Remove(1);
  EXPECT_EQ(1, ip->GetSize());
  //index_key.SetFromInteger(2);
  //EXPECT_EQ(index_key, ip->KeyAt(1));

  bpm->UnpinPage(root_page_id, true);
  bpm->UnpinPage(p0, true);
  bpm->UnpinPage(p1, true);
  bpm->UnpinPage(p2, true);
  bpm->UnpinPage(p3, true);
  bpm->UnpinPage(p4, true);
  delete disk_manager;
  delete bpm;
  delete key_schema;
  remove("test.db");
  remove("test.log");
}
TEST(BPlusTreePageTests, testLeafPage) {
  char *leaf_ptr = new char[300];
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  GenericKey<8> index_key;
  RID rid;

  BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>> *leaf = reinterpret_cast<BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>> *>(leaf_ptr);
  leaf->Init(1);
  leaf->SetMaxSize(4);

  // 测试Insert(), KeyIndex()
  index_key.SetFromInteger(3);
  EXPECT_EQ(0, leaf->KeyIndex(index_key, comparator));

  setKeyValue(1, index_key, rid);
  leaf->Insert(index_key, rid, comparator);
  EXPECT_EQ(0, leaf->KeyIndex(index_key, comparator));
  index_key.SetFromInteger(100);
  EXPECT_EQ(1, leaf->KeyIndex(index_key, comparator));

  setKeyValue(2, index_key, rid);
  leaf->Insert(index_key, rid, comparator);
  setKeyValue(3, index_key, rid);
  leaf->Insert(index_key, rid, comparator);
  setKeyValue(4, index_key, rid);
  leaf->Insert(index_key, rid, comparator);
  EXPECT_EQ(4, leaf->GetSize());
  index_key.SetFromInteger(2);
  EXPECT_EQ(1, leaf->KeyIndex(index_key, comparator));
  index_key.SetFromInteger(4);
  EXPECT_EQ(3, leaf->KeyIndex(index_key, comparator));
  index_key.SetFromInteger(100);
  EXPECT_EQ(4, leaf->KeyIndex(index_key, comparator));

  // maxSize为4，最多可以容纳5个元素，测试MoveHalfTo()
  setKeyValue(5, index_key, rid);
  leaf->Insert(index_key, rid, comparator);
  char *new_leaf_ptr = new char[300];
  BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>> *new_leaf = reinterpret_cast<BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>> *>(new_leaf_ptr);
  new_leaf->Init(2);
  new_leaf->SetMaxSize(4);
  leaf->MoveHalfTo(new_leaf, nullptr);
  EXPECT_EQ(2, leaf->GetSize());
  EXPECT_EQ(3, new_leaf->GetSize());
  EXPECT_EQ(2, leaf->GetNextPageId());

  // 测试Lookup(), 当前leaf:[(1, 1), (2, 2)], new_leaf:[(3, 3), (4, 4), (5, 5)]
  RID value;
  setKeyValue(2, index_key, rid);
  EXPECT_TRUE(leaf->Lookup(index_key, value, comparator));
  EXPECT_EQ(rid, value);
  setKeyValue(1, index_key, rid);
  EXPECT_TRUE(leaf->Lookup(index_key, value, comparator));
  EXPECT_EQ(rid, value);
  setKeyValue(5, index_key, rid);
  EXPECT_TRUE(new_leaf->Lookup(index_key, value, comparator));
  EXPECT_EQ(rid, value);
  index_key.SetFromInteger(6);
  EXPECT_FALSE(leaf->Lookup(index_key, value, comparator));

  // 测试RemoveAndDeleteRecord()
  index_key.SetFromInteger(100);
  EXPECT_EQ(2, leaf->RemoveAndDeleteRecord(index_key, comparator));
  index_key.SetFromInteger(2);
  EXPECT_EQ(1, leaf->RemoveAndDeleteRecord(index_key, comparator));

  index_key.SetFromInteger(1);
  EXPECT_EQ(0, leaf->RemoveAndDeleteRecord(index_key, comparator));
  EXPECT_EQ(0, leaf->GetSize());
  EXPECT_EQ(3, new_leaf->GetSize());

  // 测试MoveAllTo(), 当前leaf:[], new_leaf:[(3, 3),(4, 4), (5, 5)]
  new_leaf->MoveAllTo(leaf, 0, nullptr);
  EXPECT_EQ(0, new_leaf->GetSize());
  EXPECT_EQ(3, leaf->GetSize());

  delete []leaf_ptr;
  delete []new_leaf_ptr;
  delete key_schema;
}

}