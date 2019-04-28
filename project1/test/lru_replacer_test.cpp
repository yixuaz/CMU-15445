/**
 * lru_replacer_test.cpp
 */

#include <cstdio>

#include "buffer/lru_replacer.h"
#include "gtest/gtest.h"

namespace cmudb {

TEST(LRUReplacerTest, SampleTest) {
  LRUReplacer<int> lru_replacer;
  
  // push element into replacer
  lru_replacer.Insert(1);
  lru_replacer.Insert(2);
  lru_replacer.Insert(3);
  lru_replacer.Insert(4);
  lru_replacer.Insert(5);
  lru_replacer.Insert(6);
  lru_replacer.Insert(1);
  EXPECT_EQ(6, lru_replacer.Size());
  
  // pop element from replacer
  int value;
  lru_replacer.Victim(value);
  EXPECT_EQ(2, value);
  lru_replacer.Victim(value);
  EXPECT_EQ(3, value);
  lru_replacer.Victim(value);
  EXPECT_EQ(4, value);
  
  // remove element from replacer
  EXPECT_EQ(false, lru_replacer.Erase(4));
  EXPECT_EQ(true, lru_replacer.Erase(6));
  EXPECT_EQ(2, lru_replacer.Size());
  
  // pop element from replacer after removal
  lru_replacer.Victim(value);
  EXPECT_EQ(5, value);
  lru_replacer.Victim(value);
  EXPECT_EQ(1, value);
}

TEST(LRUReplacerTest, SampleTest1) {
  LRUReplacer<int> lru_replacer;
  int value;

  EXPECT_EQ(false, lru_replacer.Victim(value));

  lru_replacer.Insert(0);
  EXPECT_EQ(1, lru_replacer.Size());
  EXPECT_EQ(true, lru_replacer.Victim(value));
  EXPECT_EQ(0, value);
  EXPECT_EQ(false, lru_replacer.Victim(value));

  EXPECT_EQ(false, lru_replacer.Erase(0));
  EXPECT_EQ(0, lru_replacer.Size());

  lru_replacer.Insert(1);
  lru_replacer.Insert(1);
  lru_replacer.Insert(2);
  lru_replacer.Insert(2);
  lru_replacer.Insert(1);
  EXPECT_EQ(2, lru_replacer.Size());
  EXPECT_EQ(true, lru_replacer.Victim(value));
  EXPECT_EQ(2, value);

}

TEST(LRUReplacerTest, BasicTest) {
  LRUReplacer<int> lru_replacer;

  // push element into replacer
  for (int i = 0; i < 100; ++i) {
    lru_replacer.Insert(i);
  }
  EXPECT_EQ(100, lru_replacer.Size());

  // reverse then insert again
  for (int i = 0; i < 100; ++i) {
    lru_replacer.Insert(99 - i);
  }

  // erase 50 element from the tail
  for (int i = 0; i < 50; ++i) {
    EXPECT_EQ(true, lru_replacer.Erase(i));
  }

  // check left
  int value = -1;
  for (int i = 99; i >= 50; --i) {
    lru_replacer.Victim(value);
    EXPECT_EQ(i, value);
    value = -1;
  }
}

} // namespace cmudb
