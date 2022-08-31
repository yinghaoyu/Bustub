//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_bucket_page.cpp
//
// Identification: src/storage/page/hash_table_bucket_page.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_bucket_page.h"
#include "common/logger.h"
#include "common/util/hash_util.h"
#include "storage/index/generic_key.h"
#include "storage/index/hash_comparator.h"
#include "storage/table/tmp_tuple.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::GetValue(KeyType key, KeyComparator cmp, std::vector<ValueType> *result) {
  bool ret = false;
  for (size_t i = 0; i < BUCKET_ARRAY_SIZE; ++i) {
    if (!IsOccupied(i)) {
      break;
    }
    if (IsReadable(i) && cmp(key, array_[i].first) == 0) {
      result->push_back(array_[i].second);
      ret = true;
    }
  }
  return ret;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Insert(KeyType key, ValueType value, KeyComparator cmp) {
  int64_t free_slot = -1;
  for (size_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (IsReadable(i)) {
      if (cmp(key, array_[i].first) == 0 && value == array_[i].second) {
        // already existed the same key & value
        //                LOG_DEBUG("Same kv");
        return false;
      }
    } else if (free_slot == -1) {
      free_slot = i;
    }
  }

  if (free_slot == -1) {
    // is full
    LOG_DEBUG("Bucket is full");
    return false;
  }

  // insert it and return true
  SetOccupied(free_slot);
  SetReadable(free_slot);
  array_[free_slot] = MappingType(key, value);
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Remove(KeyType key, ValueType value, KeyComparator cmp) {
  for (size_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (IsReadable(i)) {
      if (cmp(key, array_[i].first) == 0 && value == array_[i].second) {
        // find it
        RemoveAt(i);
        return true;
      }
    }
  }
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
KeyType HASH_TABLE_BUCKET_TYPE::KeyAt(uint32_t bucket_idx) const {
  return array_[bucket_idx].first;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
ValueType HASH_TABLE_BUCKET_TYPE::ValueAt(uint32_t bucket_idx) const {
  return array_[bucket_idx].second;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::RemoveAt(uint32_t bucket_idx) {
  // Lazy remove
  SetReadable(bucket_idx, false);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsOccupied(uint32_t bucket_idx) const {
  size_t idx = bucket_idx >> 3;       // same as bucket_idx / 8
  size_t offset = bucket_idx & 0x07;  // same as bucket_idx % 8
  return (occupied_[idx] & (1 << offset)) != 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetOccupied(uint32_t bucket_idx) {
  SetOccupied(bucket_idx, true);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetOccupied(uint32_t bucket_idx, bool flag) {
  size_t idx = bucket_idx >> 3;
  size_t offset = bucket_idx & 0x07;
  if (flag) {
    occupied_[idx] |= (1 << offset);
  } else {
    occupied_[idx] &= ~(1 << offset);
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsReadable(uint32_t bucket_idx) const {
  size_t idx = bucket_idx >> 3;       // same as bucket_idx / 8
  size_t offset = bucket_idx & 0x07;  // same as bucket_idx % 8
  return (readable_[idx] & (1 << offset)) != 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetReadable(uint32_t bucket_idx) {
  SetReadable(bucket_idx, true);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetReadable(uint32_t bucket_idx, bool flag) {
  size_t idx = bucket_idx >> 3;
  size_t offset = bucket_idx & 0x07;
  if (flag) {
    readable_[idx] |= (1 << offset);
  } else {
    readable_[idx] &= ~(1 << offset);
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsFull() {
  size_t size = BUCKET_ARRAY_SIZE >> 3;
  for (size_t i = 0; i < size; i++) {
    if (static_cast<uint8_t>(readable_[i]) != 0xff) {
      return false;
    }
  }
  // Special for the last remaining element
  size_t remainder = (BUCKET_ARRAY_SIZE & 0x07);
  if (remainder > 0) {
    return readable_[size] == ((1 << remainder) - 1);
  }
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_BUCKET_TYPE::NumReadable() {
  // BitCount
  // https://github.com/yinghaoyu/csapp/blob/master/src/bit_manipulation.c
  auto f = [](uint8_t x) -> uint8_t {
    uint8_t c;
    //  0x55   =     0101 0101    2 bits peer group
    //  0x33   =     0011 0011    4 bits peer group
    //  0x0f   =     0000 ffff    8 bits peer group
    c = (x & 0x55) + ((x >> 1) & 0x55);
    c = (c & 0x33) + ((c >> 2) & 0x33);
    c = (c & 0x0f) + ((c >> 4) & 0x0f);
    return c;
  };

  uint32_t num = 0;
  size_t size = BUCKET_ARRAY_SIZE >> 3;

  for (size_t i = 0; i < size; i++) {
    uint8_t ic = static_cast<uint8_t>(readable_[i]);
    num += f(ic);
  }
  // Special for the last remaining element
  size_t remainder = BUCKET_ARRAY_SIZE & 0x07;
  if (remainder > 0) {
    uint8_t ic = static_cast<uint8_t>(readable_[size]);
    num += f(ic);
  }
  return num;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsEmpty() {
  size_t size = sizeof(readable_) / sizeof(decltype(readable_));
  for (size_t i = 0; i < size; i++) {
    if (static_cast<uint8_t>(readable_[i]) != 0) {
      return false;
    }
  }
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
std::vector<MappingType> HASH_TABLE_BUCKET_TYPE::GetKeyValueCopy() {
  std::vector<MappingType> copy;
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (IsReadable(i)) {
      copy.push_back(array_[i]);
    }
  }
  assert(copy.size() == NumReadable());
  return copy;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::PrintBucket() {
  uint32_t size = 0;
  uint32_t taken = 0;
  uint32_t free = 0;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }

    size++;

    if (IsReadable(bucket_idx)) {
      taken++;
    } else {
      free++;
    }
  }

  LOG_INFO("Bucket Capacity: %lu, Size: %u, Taken: %u, Free: %u", BUCKET_ARRAY_SIZE, size, taken, free);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HashTableBucketPage<KeyType, ValueType, KeyComparator>::Clear() {
  LOG_DEBUG("clear");
  memset(occupied_, 0, sizeof(occupied_));
  memset(readable_, 0, sizeof(readable_));
  memset(array_, 0, sizeof(array_));
}

// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBucketPage<int, int, IntComparator>;

template class HashTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

// template class HashTableBucketPage<hash_t, TmpTuple, HashComparator>;

}  // namespace bustub
