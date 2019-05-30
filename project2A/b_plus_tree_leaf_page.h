/**
 * b_plus_tree_leaf_page.h
 *
 * Store indexed key and record id(record id = page id combined with slot id,
 * see include/common/rid.h for detailed implementation) together within leaf
 * page. Only support unique key.

 * Leaf page format (keys are stored in order):
 *  ----------------------------------------------------------------------
 * | HEADER | KEY(1) + RID(1) | KEY(2) + RID(2) | ... | KEY(n) + RID(n)
 *  ----------------------------------------------------------------------
 *
 *  Header format (size in byte, 24 bytes in total):
 *  ---------------------------------------------------------------------
 * | PageType (4) | CurrentSize (4) | MaxSize (4) | ParentPageId (4) |
 *  ---------------------------------------------------------------------
 *  ------------------------------
 * | PageId (4) | NextPageId (4)
 *  ------------------------------
 */
#pragma once
#include <utility>
#include <vector>

#include "page/b_plus_tree_page.h"

namespace cmudb {
#define B_PLUS_TREE_LEAF_PAGE_TYPE                                             \
  BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeLeafPage : public BPlusTreePage {

public:
  // After creating a new leaf page from buffer pool, must call initialize
  // method to set default values
  void Init(page_id_t page_id, page_id_t parent_id = INVALID_PAGE_ID);
  // helper methods
  page_id_t GetNextPageId() const;
  void SetNextPageId(page_id_t next_page_id);
  KeyType KeyAt(int index) const;
  int KeyIndex(const KeyType &key, const KeyComparator &comparator) const;
  const MappingType &GetItem(int index);

  // insert and delete methods
  int Insert(const KeyType &key, const ValueType &value,
             const KeyComparator &comparator);
  bool Lookup(const KeyType &key, ValueType &value,
              const KeyComparator &comparator) const;
  int RemoveAndDeleteRecord(const KeyType &key,
                            const KeyComparator &comparator);
  // Split and Merge utility methods
  void MoveHalfTo(BPlusTreeLeafPage *recipient,
                  BufferPoolManager *buffer_pool_manager /* Unused */);
  void MoveAllTo(BPlusTreeLeafPage *recipient, int /* Unused */,
                 BufferPoolManager * /* Unused */);
  void MoveFirstToEndOf(BPlusTreeLeafPage *recipient,
                        BufferPoolManager *buffer_pool_manager);
  void MoveLastToFrontOf(BPlusTreeLeafPage *recipient, int parentIndex,
                         BufferPoolManager *buffer_pool_manager);
  // Debug
  std::string ToString(bool verbose = false) const;

private:
  void CopyHalfFrom(MappingType *items, int size);
  void CopyAllFrom(MappingType *items, int size);
  void CopyLastFrom(const MappingType &item);
  void CopyFirstFrom(const MappingType &item, int parentIndex,
                     BufferPoolManager *buffer_pool_manager);
  page_id_t next_page_id_;
  MappingType array[0];
};
} // namespace cmudb
