#include <list>

#include "hash/extendible_hash.h"
#include "page/page.h"

namespace cmudb {

/*
 * constructor
 * array_size: fixed array size for each bucket
 */
template <typename K, typename V>
ExtendibleHash<K, V>::ExtendibleHash(size_t size) :
	numBuckets(0), bucketsize(size), globalDepth(0) {
		bucket_directory.emplace_back(std::make_shared<Bucket>); 			//using make_shared to allocate memory for a shared_ptr
		numBuckets += 1;										 			//add num + 1
	}

/*
 * helper function to calculate the hashing address of input key
 */
template <typename K, typename V>
size_t ExtendibleHash<K, V>::HashKey(const K &key) {
  return std::hash<K>()(key);	
}

/*
 * helper function to return global depth of hash table
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetGlobalDepth() const {
  	std::lock_guard<std::mutex> lock(big_latch);							//lock the entire hash_table
  	return globalDepth;
}

/*
 * helper function to return local depth of one specific bucket
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetLocalDepth(int bucket_id) const {
 	std::lock_guard<std::mutex> lock(bucket_directory[bucket_id] -> latch);	//instead of locking the entire hash table, just lock the bucket
  	return bucket_directory[bucket_id] -> localDepth;
}

/*
 * helper function to return current number of bucket in hash table
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetNumBuckets() const {
 	std::lock_guard<std::mutex> lock(big_latch);							//lock the entire hash table
  	return numBuckets;
}

/*
 * lookup function to find value associate with input key
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Find(const K &key, V &value) {
 	std::lock_guard<std::mutex> lock(big_latch); 	  						// to find an entry, you first need to find which bucket stores it
  	size_t id = HashKey(key) & ((1 << globalDepth) - 1); 					// for that, hash the key ans use the first 'm' LSBs, m stands for the globalDepth
  	if (bucket_directory[id] != nullptr) {			  						// if this is not a nullptr proceed
  		auto container = bucket_directory[id];		  						// this is the bucket we were looking for
  		if (container -> pairs.find(key) != container -> pairs.end()) { 	// find the key in this bucket, ensure it is present
  			value = container -> pairs[key];	  	  						// store the value associated with the key
  				return true;                     					    	// return true if we could perform this operation
  			}
  		}
  	}
  	return false;
}

/*
 * delete <key,value> entry in hash table
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Remove(const K &key) {
 	size_t id = HashKey(key) & ((1 << globalDepth) - 1); 					// again, find the id for the bucket which stores this key val pair
 	if (bucket_directory[id]) {												// if a bucket at this bucket directory exists
	 	std::lock_guard<std::mutex> lock(bucket_directory[id] -> latch);	// let's lock this bucket so no-one else can modify it
	 	auto container = bucket_directory[id];								// this is the container/bucket where the key value pair is stored
	 	if (container -> pairs.find(key) != container -> pairs.end()) { 	// find the pair with the key
	 	 	pairs.erase(key);												// erase it
	 	 	return true;													// voila!
	 	}
	 	else																// couldn't find the pair
	 		return false;													// it doesn't exist, return false
	}
	 	
 	else
 		return false;														// if the required bucket doesn't exist
}

/*
 * insert <key,value> entry in hash table
 * Split & Redistribute bucket when there is overflow and if necessary increase
 * global depth
 */
template <typename K, typename V>
void ExtendibleHash<K, V>::Insert(const K &key, const V &value) {
 	std::lock_guard<std::mutex> lock(big_latch);							// lock the bucket directory
 	size_t id = HashKey(key) & ((1 << globalDepth) - 1); 					// find the id of the bucket where this key should go
 	if (bucket_directory[id]) {												// if this bucket exists, enter
	 	auto curr = bucket_directory[id];		  							// this is the prospective bucket for this key
	 	while (true) {
	 		if(curr -> pages.find(key) != curr.pages.end() || curr -> pages.size() < bucketsize) {
	 			curr -> pages[key] = value;									// the key already exists or no overflow
	 			break;														// exit
	 		}
	 	 	auto mask_bit = 1 << (curr -> localDepth);						// use mask bit to split the bucket directory later
	 	 	curr -> localDepth++;							
	 	 	if (curr -> localDepth > globalDepth) {							// overflow!
	 	 		globalDepth++;					
	 	 		int length = bucket_directory.size();
	 	 		for (int i = 0; i < length; i++) {							// double the size of the bucket directory, the next slots sequentially point to the same buckets as the first half 
	 	 			bucket_directory.push_back(bucket_directory[i]);
	 	 		}
	 	 	}
	 	 	numBuckets += 1;												// there is one more bucket now
	 	 	auto newBucketallocated = std::make_shared<Bucket>(curr -> localDepth);
	 	 	typename std::map<K, V>::iterator iter;							// splitting the bucket, start
	 	 	for (iter = curr -> pages.begin(); iter != curr -> pages.end();) {
	 	 		if (HashKey(iter -> first) & mask_bit) {					// transferring the key value pair to the new bucket
	 	 			newBucketallocated -> pages[iter -> first] = iter -> second;
	 	 			iter = curr -> pages.erase(iter);						// erasing this key value pair from the old bucket
	 	 		}
	 	 		else {
	 	 			iter++;													// else, move forward
	 	 		}
	 	 	}
	 	 	
	 	 	for (size_t i = 0; i < bucket_directory.size(); i++) {			// chnage the pointer to the new bucket
	 	 		if (bucket_directory[i] == curr && (i & mask_bit)) {
	 	 			bucket_directory[i] = newBucketallocated;
	 	 		}
	 	 	}
	 	}
	 	
	 	id = HashKey(key) & ((1 << globalDepth) - 1);
	 	curr = bucket_directory[id];
	}
	else return;
}

template class ExtendibleHash<page_id_t, Page *>;
template class ExtendibleHash<Page *, std::list<Page *>::iterator>;
// test purpose
template class ExtendibleHash<int, std::string>;
template class ExtendibleHash<int, std::list<int>::iterator>;
template class ExtendibleHash<int, int>;
} // namespace cmudb
