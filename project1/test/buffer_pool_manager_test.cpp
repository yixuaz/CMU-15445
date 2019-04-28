/**
 * buffer_pool_manager_test.cpp
 */

#include <cstdio>

#include "buffer/buffer_pool_manager.h"
#include "gtest/gtest.h"

namespace cmudb {

TEST(BufferPoolManagerTest, SampleTest) {
  page_id_t temp_page_id;

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager bpm(10, disk_manager);

  auto page_zero = bpm.NewPage(temp_page_id);
  EXPECT_NE(nullptr, page_zero);
  EXPECT_EQ(0, temp_page_id);

  // The test will fail here if the page is null
  ASSERT_NE(nullptr, page_zero);

  // change content in page one
  strcpy(page_zero->GetData(), "Hello");

  for (int i = 1; i < 10; ++i) {
    EXPECT_NE(nullptr, bpm.NewPage(temp_page_id));
  }
  // all the pages are pinned, the buffer pool is full
  for (int i = 10; i < 15; ++i) {
    EXPECT_EQ(nullptr, bpm.NewPage(temp_page_id));
  }
  // upin the first five pages, add them to LRU list, set as dirty
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(true, bpm.UnpinPage(i, true));
  }
  // we have 5 empty slots in LRU list, evict page zero out of buffer pool
  for (int i = 10; i < 14; ++i) {
    EXPECT_NE(nullptr, bpm.NewPage(temp_page_id));
  }
  // fetch page one again
  page_zero = bpm.FetchPage(0);
  // check read content
  EXPECT_EQ(0, strcmp(page_zero->GetData(), "Hello"));

  remove("test.db");
}

TEST(BufferPoolManagerTest, SampleTest2) {
  page_id_t temp_page_id;

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager bpm(10, disk_manager);

  auto page_zero = bpm.NewPage(temp_page_id);
  EXPECT_NE(nullptr, page_zero);
  EXPECT_EQ(0, temp_page_id);

  // The test will fail here if the page is null
  ASSERT_NE(nullptr, page_zero);

  // change content in page one
  strcpy(page_zero->GetData(), "Hello");

  for (int i = 1; i < 10; ++i) {
    EXPECT_NE(nullptr, bpm.NewPage(temp_page_id));
  }

  // upin the first five pages, add them to LRU list, set as dirty
  for (int i = 0; i < 1; ++i) {
    EXPECT_EQ(true, bpm.UnpinPage(i, true));
    page_zero = bpm.FetchPage(0);
    EXPECT_EQ(0, strcmp(page_zero->GetData(), "Hello"));
    EXPECT_EQ(true, bpm.UnpinPage(i, true));
    EXPECT_NE(nullptr, bpm.NewPage(temp_page_id));
  }

  std::vector<int> test{5, 6, 7, 8, 9, 10};

  for (auto v: test) {
    Page* page = bpm.FetchPage(v);
    if (page == nullptr) {
      assert(false);
    }
    EXPECT_EQ(v, page->GetPageId());
    bpm.UnpinPage(v, true);
  }

  bpm.UnpinPage(10, true);

  // fetch page one again
  page_zero = bpm.FetchPage(0);
  // check read content
  EXPECT_EQ(0, strcmp(page_zero->GetData(), "Hello"));

  remove("test.db");
}


} // namespace cmudb
