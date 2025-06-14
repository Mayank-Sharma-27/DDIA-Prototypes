import os
import json
import time
# To install the bloom filter library: pip install pybloom-live
from pybloom_live import BloomFilter

# --- Design Constants ---

# A unique object used to mark a key as deleted in the database.
# Using a unique object instance is safer than using a common value like None or "",
# as those could be legitimate values for a key. This is a common practice in
# systems programming to avoid ambiguity.
TOMBSTONE = object()

# The file extension for our Sorted String Table files.
SSTABLE_EXTENSION = ".sst"
# The file extension for our Bloom Filter files.
BLOOM_FILTER_EXTENSION = ".bf"

class LSMTree:
    """
    A prototype of a Log-Structured Merge-Tree (LSM Tree).

    This implementation demonstrates the core principles of an LSM Tree, including:
    - An in-memory write buffer (Memtable) for extremely fast writes.
    - Flushing the Memtable to sorted, immutable on-disk files (SSTables).
    - Tombstones for handling deletions.
    - A multi-layered read path that checks memory before disk.

    It also includes two critical optimizations for read performance:
    1.  Bloom Filters: To quickly determine if a key *does not* exist in an SSTable,
        avoiding unnecessary disk reads.
    2.  Sparse Indexes: To find the approximate location of a key within an SSTable,
        drastically reducing the amount of data that needs to be scanned on disk.
    """

    def __init__(self, directory, memtable_threshold=10, sparse_index_granularity=2):
        """
        Initializes the LSM Tree.

        Args:
            directory (str): The directory to store the data files (SSTables).
            memtable_threshold (int): The max number of entries in the memtable
                                     before it's flushed to disk.
            sparse_index_granularity (int): Determines how often to sample keys for the
                                            sparse index (e.g., every 2nd key).
        """
        self._dir = directory
        if not os.path.exists(self._dir):
            os.makedirs(self._dir)
        
        self.memtable_threshold = memtable_threshold
        self.sparse_index_granularity = sparse_index_granularity

        # The Memtable: an in-memory buffer for recent writes. It's a key component
        # for achieving high write throughput, as all writes go to memory first.
        # In a production system, this would be a more complex concurrent data
        # structure like a SkipList or a Red-Black Tree.
        self.memtable = {}
        
        # --- In-Memory Indexes for Read Optimization ---
        # These data structures are held in memory to make reads faster. They are
        # rebuilt at startup by scanning the files on disk.

        # A cache for Bloom Filters, one for each SSTable.
        # Maps sstable_filename -> BloomFilter object.
        self._bloom_filters = {}

        # A cache for Sparse Indexes.
        # Maps sstable_filename -> sparse_index_dictionary.
        self._sparse_indexes = {}
        
        # A list of SSTable file paths, kept sorted from newest to oldest.
        self._sstables = self._load_from_disk()

    def _load_from_disk(self):
        """
        Scans the data directory to load SSTable metadata and indexes into memory.

        This is a critical startup process. It ensures the database can restore
        its state and operate efficiently without needing to re-scan files on
        every query.
        """
        sstables = []
        for filename in os.listdir(self._dir):
            if filename.endswith(SSTABLE_EXTENSION):
                sstable_name = filename.replace(SSTABLE_EXTENSION, "")
                sstables.append(sstable_name)

                # Load the corresponding Bloom Filter into memory.
                bloom_filter_path = os.path.join(self._dir, sstable_name + BLOOM_FILTER_EXTENSION)
                with open(bloom_filter_path, 'rb') as f:
                    self._bloom_filters[sstable_name] = BloomFilter.fromfile(f)

                # Rebuild the sparse index from the SSTable file.
                self._sparse_indexes[sstable_name] = self._rebuild_sparse_index(sstable_name)

        # Sort by the timestamp in the filename in descending order (newest first).
        # This is crucial for the read path, ensuring we always find the most
        # recent version of a key first.
        return sorted(sstables, reverse=True)
    
    def _rebuild_sparse_index(self, sstable_name):
        """Recreates the sparse index for a given SSTable by scanning it."""
        sparse_index = {}
        sstable_path = os.path.join(self._dir, sstable_name + SSTABLE_EXTENSION)
        offset = 0
        entry_count = 0
        with open(sstable_path, 'r') as f:
            while True:
                line = f.readline()
                if not line:
                    break
                
                if entry_count % self.sparse_index_granularity == 0:
                    record = json.loads(line)
                    sparse_index[record['key']] = offset
                
                offset = f.tell()
                entry_count += 1
        return sparse_index

    def put(self, key, value):
        """
        Stores a key-value pair.

        First writes to the in-memory Memtable. If the Memtable exceeds its
        threshold, it's flushed to a new SSTable on disk. This is the "Log"
        part of the "Log-Structured" name, as writes are buffered.
        """
        self.memtable[key] = value
        if len(self.memtable) >= self.memtable_threshold:
            self._flush()

    def get(self, key):
        """
        Retrieves the value for a given key using the optimized LSM read path.

        This demonstrates the core LSM Tree read-path hierarchy, which is designed
        to minimize slow disk I/O by checking faster layers first.

        Returns:
            The value associated with the key, or None if the key is not found
            or has been deleted.
        """
        # --- Layer 1: Check the Memtable ---
        # This is the fastest possible read, as the data is already in RAM.
        if key in self.memtable:
            value = self.memtable[key]
            # If the most recent entry is a tombstone, the key is considered deleted.
            return None if value is TOMBSTONE else value

        # --- Layer 2: Check On-Disk SSTables (from newest to oldest) ---
        for sstable_name in self._sstables:
            # --- Optimization 1: Bloom Filter Check ---
            # Ask the Bloom Filter if the key *might* be in this file.
            # If it returns False, the key is *definitely not* in this SSTable.
            # This allows us to completely skip a disk read for this file, which is a
            # massive performance win for non-existent keys.
            if key not in self._bloom_filters[sstable_name]:
                continue # Skip to the next SSTable

            # --- Optimization 2: Sparse Index Lookup ---
            # If the Bloom Filter gave a potential positive, we use the sparse index
            # to find the small "block" on disk where the key *should* be.
            start_offset = self._find_block_offset(sstable_name, key)
            
            # Now, we only need to scan this small block instead of the whole file.
            sstable_path = os.path.join(self._dir, sstable_name + SSTABLE_EXTENSION)
            with open(sstable_path, 'r') as f:
                f.seek(start_offset)
                for line in f:
                    record = json.loads(line)
                    # Since SSTables are sorted, if we see a key that is alphabetically
                    # greater than our target key, we know the target key is not in this
                    # file. We can stop scanning this block early.
                    if record['key'] > key:
                        break # Short-circuit scan
                    
                    if record['key'] == key:
                        # Found the key. If it's a tombstone, it's deleted.
                        return None if record['value'] is None else record['value']

        # --- Layer 3: Not Found ---
        # If the key wasn't found in the memtable or any SSTable, it doesn't exist.
        return None
    
    def _find_block_offset(self, sstable_name, key):
        """
        Uses the sparse index to find the starting disk offset for the block
        containing the given key.
        """
        sparse_index = self._sparse_indexes[sstable_name]
        
        # Find the largest key in the sparse index that is less than or equal to the target key.
        # This gives us the starting point of the block to scan.
        relevant_keys = [k for k in sparse_index.keys() if k <= key]
        if not relevant_keys:
            return 0 # Scan from the beginning of the file
        
        start_key = max(relevant_keys)
        return sparse_index[start_key]

    def delete(self, key):
        """
        Deletes a key by writing a special "tombstone" marker.

        This is a "soft delete." The key isn't immediately removed from disk.
        Instead, a record is written that signals the key is deleted. The actual
        removal of the key and its old values happens later during compaction.
        """
        self.put(key, TOMBSTONE)
    
    def _flush(self):
        """
        Flushes the Memtable to a new SSTable, Bloom Filter, and Sparse Index.
        This is a write-intensive operation but is done sequentially for speed.
        """
        if not self.memtable:
            return

        # Use a high-resolution timestamp for the filename to ensure uniqueness.
        timestamp = int(time.time() * 1_000_000)
        sstable_name = str(timestamp)
        sstable_path = os.path.join(self._dir, sstable_name + SSTABLE_EXTENSION)
        
        # Sort the memtable items by key. This is the "S" in SSTable (Sorted String Table).
        # This sorting is what allows for efficient range scans and sparse index lookups.
        sorted_items = sorted(self.memtable.items())

        # Prepare the Bloom Filter for this new SSTable.
        # We need to estimate the capacity and a desired error rate.
        bloom_filter = BloomFilter(capacity=len(sorted_items), error_rate=0.01)
        sparse_index = {}

        with open(sstable_path, 'w') as f:
            entry_count = 0
            for key, value in sorted_items:
                # Add the key to our Bloom Filter.
                bloom_filter.add(key)

                current_offset = f.tell()
                # Sample every Nth key for our sparse index.
                if entry_count % self.sparse_index_granularity == 0:
                    sparse_index[key] = current_offset
                
                # Use `None` in JSON to represent our TOMBSTONE object.
                json_value = None if value is TOMBSTONE else value
                record = {'key': key, 'value': json_value}
                f.write(json.dumps(record) + '\n')
                entry_count += 1
                
        print(f"Flushed Memtable to {sstable_name}{SSTABLE_EXTENSION}")
        
        # --- Persist the In-Memory Indexes to Disk ---
        
        # Save the Bloom Filter.
        bloom_filter_path = os.path.join(self._dir, sstable_name + BLOOM_FILTER_EXTENSION)
        with open(bloom_filter_path, 'wb') as f:
            bloom_filter.tofile(f)

        # --- Update Live In-Memory State ---
        
        # Add the new SSTable to our list (at the front, since it's the newest).
        self._sstables.insert(0, sstable_name)
        # Cache the new indexes in memory for immediate use.
        self._bloom_filters[sstable_name] = bloom_filter
        self._sparse_indexes[sstable_name] = sparse_index
        
        # Clear the memtable to accept new writes.
        self.memtable.clear()       
                 
# --- Example Usage ---
if __name__ == '__main__':
    # Clean up previous runs
    data_dir = "lsm_data_optimized"
    if os.path.exists(data_dir):
        import shutil
        shutil.rmtree(data_dir)

    print("--- Initializing LSM Tree ---")
    db = LSMTree(data_dir, memtable_threshold=4)

    # These first 3 writes will stay in the Memtable
    db.put("name", "Alice")
    db.put("city", "New York")
    db.put("language", "Python")
    print("Memtable contents:", db.memtable)
    
    # This 4th write will trigger a flush
    print("\n--- Adding 4th element to trigger flush ---")
    db.put("job", "Engineer")
    print("Memtable is now empty:", db.memtable)
    print("Current SSTables:", db._sstables)

    # Add more data, this time updating a key
    db.put("city", "San Francisco") # This will live in the memtable
    db.put("status", "Active")
    db.put("country", "USA")
    
    # This will trigger another flush
    print("\n--- Adding 8th element to trigger flush ---")
    db.put("system", "LSMTree") 
    print("Current SSTables:", db._sstables)
    
    # Demonstrate reads
    print("\n--- Reading data ---")
    print(f"Name: {db.get('name')}") # Read from the first (newest) SSTable
    print(f"City: {db.get('city')}") # Read from the Memtable (most recent value)
    print(f"System: {db.get('system')}") # Read from the second SSTable
    print(f"Non-existent key 'foo': {db.get('foo')}") # Will be fast due to Bloom filters

    # Demonstrate deletion
    print("\n--- Deleting data ---")
    db.delete("language") # 'language' now has a tombstone in the memtable
    print(f"Language after deletion (read from memtable): {db.get('language')}")

    # Flush the deletion marker to disk
    db.put("a","1")
    db.put("b","2")
    db.put("c","3") # This should trigger the final flush

    print("\n--- Final State ---")
    print("Memtable is now empty:", db.memtable)
    print("SSTables:", db._sstables)
    print(f"Language should still be None after flush: {db.get('language')}")