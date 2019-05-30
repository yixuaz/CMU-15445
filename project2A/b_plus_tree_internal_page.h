/**
 * b_plus_tree_internal_page.h
 *
 * Store n indexed keys and n+1 child pointers (page_id) within internal page.
 * Pointer PAGE_ID(i) points to a subtree in which all keys K satisfy:
 * K(i) <= K < K(i+1).
 * NOTE: since the number of keys does not equal to number of child pointers,
 * the first key always remains invalid. That is to say, any search/lookup
 * should ignore the first key.
 *
 * Internal page format (keys are stored in increasing order):
 *  --------------------------------------------------------------------------
 * | HEADER | KEY(1)+PAGE_ID(1) | KEY(2)+PAGE_ID(2) | ... | KEY(n)+PAGE_ID(n) |
 *  --------------------------------------------------------------------------
 */

#pragma once

#include <queue>

#include "page/b_plus_tree_page.h"

namespace cmudb {

#define B_PLUS_TREE_INTERNAL_PAGE_TYPE                                         \
  BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>

#define B_PLUS_TREE_INTERNAL_PAGE  BPlusTreeInternalPage <KeyType, page_id_t, KeyComparator>
  
INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeInternalPage : public BPlusTreePage {
public:
  // must call initialize method after "create" a new node
  void Init(page_id_t page_id, page_id_t parent_id = INVALID_PAGE_ID);

  KeyType KeyAt(int index) const;
  void SetKeyAt(int index, const KeyType &key);
  int ValueIndex(const ValueType &value) const;
  ValueType ValueAt(int index) const;

  ValueType Lookup(const KeyType &key, const KeyComparator &comparator) const;
  void PopulateNewRoot(const ValueType &old_value, const KeyType &new_key,
                       const ValueType &new_value);
  int InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                      const ValueType &new_value);
  void Remove(int index);
  ValueType RemoveAndReturnOnlyChild();

  void MoveHalfTo(BPlusTreeInternalPage *recipient,
                  BufferPoolManager *buffer_pool_manager);
  void MoveAllTo(BPlusTreeInternalPage *recipient, int index_in_parent,
                 BufferPoolManager *buffer_pool_manager);
  void MoveFirstToEndOf(BPlusTreeInternalPage *recipient,
                        BufferPoolManager *buffer_pool_manager);
  void MoveLastToFrontOf(BPlusTreeInternalPage *recipient,
                         int parent_index,
                         BufferPoolManager *buffer_pool_manager);
  // DEUBG and PRINT
  std::string ToString(bool verbose) const;
  void QueueUpChildren(std::queue<BPlusTreePage *> *queue,
                       BufferPoolManager *buffer_pool_manager);

private:
  void CopyHalfFrom(MappingType *items, int size,
                    BufferPoolManager *buffer_pool_manager);
  void CopyAllFrom(MappingType *items, int size,
                   BufferPoolManager *buffer_pool_manager);
  void CopyLastFrom(const MappingType &pair,
                    BufferPoolManager *buffer_pool_manager);
  void CopyFirstFrom(const MappingType &pair, int parent_index,
                     BufferPoolManager *buffer_pool_manager);
  MappingType array[0];
};
} // namespace cmudb
