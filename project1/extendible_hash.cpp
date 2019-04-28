#include <list>

#include "hash/extendible_hash.h"
#include "page/page.h"
using namespace std;

namespace cmudb {

/*
 * constructor
 * array_size: fixed array size for each bucket
 */
template <typename K, typename V>
ExtendibleHash<K, V>::ExtendibleHash(size_t size) :  globalDepth(0),bucketSize(size),bucketNum(1) {
  buckets.push_back(make_shared<Bucket>(0));
}
template<typename K, typename V>
ExtendibleHash<K, V>::ExtendibleHash() {
  ExtendibleHash(64);
}

/*
 * helper function to calculate the hashing address of input key
 */
template <typename K, typename V>
size_t ExtendibleHash<K, V>::HashKey(const K &key) const{
  return hash<K>{}(key);
}

/*
 * helper function to return global depth of hash table
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetGlobalDepth() const{
  lock_guard<mutex> lock(latch);
  return globalDepth;
}

/*
 * helper function to return local depth of one specific bucket
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetLocalDepth(int bucket_id) const {
  //lock_guard<mutex> lck2(latch);
  if (buckets[bucket_id]) {
    lock_guard<mutex> lck(buckets[bucket_id]->latch);
    if (buckets[bucket_id]->kmap.size() == 0) return -1;
    return buckets[bucket_id]->localDepth;
  }
  return -1;
}

/*
 * helper function to return current number of bucket in hash table
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetNumBuckets() const{
  lock_guard<mutex> lock(latch);
  return bucketNum;
}

/*
 * lookup function to find value associate with input key
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Find(const K &key, V &value) {

  int idx = getIdx(key);
  lock_guard<mutex> lck(buckets[idx]->latch);
  if (buckets[idx]->kmap.find(key) != buckets[idx]->kmap.end()) {
    value = buckets[idx]->kmap[key];
    return true;

  }
  return false;
}

template <typename K, typename V>
int ExtendibleHash<K, V>::getIdx(const K &key) const{
  lock_guard<mutex> lck(latch);
  return HashKey(key) & ((1 << globalDepth) - 1);
}

/*
 * delete <key,value> entry in hash table
 * Shrink & Combination is not required for this project
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Remove(const K &key) {
  int idx = getIdx(key);
  lock_guard<mutex> lck(buckets[idx]->latch);
  shared_ptr<Bucket> cur = buckets[idx];
  if (cur->kmap.find(key) == cur->kmap.end()) {
    return false;
  }
  cur->kmap.erase(key);
  return true;
}

/*
 * insert <key,value> entry in hash table
 * Split & Redistribute bucket when there is overflow and if necessary increase
 * global depth
 */
template <typename K, typename V>
void ExtendibleHash<K, V>::Insert(const K &key, const V &value) {
  int idx = getIdx(key);
  shared_ptr<Bucket> cur = buckets[idx];
  while (true) {
    lock_guard<mutex> lck(cur->latch);
    if (cur->kmap.find(key) != cur->kmap.end() || cur->kmap.size() < bucketSize) {
      cur->kmap[key] = value;
      break;
    }
    int mask = (1 << (cur->localDepth));
    cur->localDepth++;

    {
      lock_guard<mutex> lck2(latch);
      if (cur->localDepth > globalDepth) {

        size_t length = buckets.size();
        for (size_t i = 0; i < length; i++) {
          buckets.push_back(buckets[i]);
        }
        globalDepth++;

      }
      bucketNum++;
      auto newBuc = make_shared<Bucket>(cur->localDepth);

      typename map<K, V>::iterator it;
      for (it = cur->kmap.begin(); it != cur->kmap.end(); ) {
        if (HashKey(it->first) & mask) {
          newBuc->kmap[it->first] = it->second;
          it = cur->kmap.erase(it);
        } else it++;
      }
      for (size_t i = 0; i < buckets.size(); i++) {
        if (buckets[i] == cur && (i & mask))
          buckets[i] = newBuc;
      }
    }
    idx = getIdx(key);
    cur = buckets[idx];
  }
}



template class ExtendibleHash<page_id_t, Page *>;
template class ExtendibleHash<Page *, std::list<Page *>::iterator>;
// test purpose
template class ExtendibleHash<int, std::string>;
template class ExtendibleHash<int, std::list<int>::iterator>;
template class ExtendibleHash<int, int>;
} // namespace cmudb
