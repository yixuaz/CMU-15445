/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "page/b_plus_tree_leaf_page.h"

namespace cmudb {

#define INDEXITERATOR_TYPE                                                     \
  IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
public:
  // you may define your own constructor based on your member variables
  IndexIterator(B_PLUS_TREE_LEAF_PAGE_TYPE *leaf, int index, BufferPoolManager *bufferPoolManager);
  ~IndexIterator();

  bool isEnd(){
    return (leaf_ == nullptr) || (index_ >= leaf_->GetSize());
  }

  const MappingType &operator*() {
    return leaf_->GetItem(index_);
  }

  IndexIterator &operator++() {
    index_++;
    if (index_ >= leaf_->GetSize()) {
      page_id_t next = leaf_->GetNextPageId();
      if (next == INVALID_PAGE_ID) {
        leaf_ = nullptr;
      } else {
        bufferPoolManager_->UnpinPage(leaf_->GetPageId(), false);
        Page *page = bufferPoolManager_->FetchPage(next);
        leaf_ = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page->GetData());
        index_ = 0;
      }
    }
    return *this;
  }

private:
  // add your own private member variables here
  int index_;
  B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_;
  BufferPoolManager *bufferPoolManager_;
};

} // namespace cmudb
