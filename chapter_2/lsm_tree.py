import os
import json
import time
class LSMTree:
    def __init__(self, directory, memtable_threshold=20):
        
        self._dir = directory
        if not os.path.exists(self._dir):
            os.makedirs(self._dir)
        
        self.memtable_threshold = memtable_threshold
        self.memtable = {}
        self._sstables = self._load_sstables()
        
    def _load_sstables(self):
        files = [f for f in os.listdir(self._dir) if f.endswith(".jsonl")]
        return sorted(files, reverse=True)    
    
    def delete(self, key):
        """
        Deletes a key by writing a special "tombstone" marker.
        """
        self.put(key, "")
    
    def put(self, key, value):
        self.memtable[key] = value
        if len(self.memtable) > self.memtable_threshold:
            self._flush
    
    ## This becomes super slow at the current implementation
    def get(self, key):
        if self.memtable[key] is not None:
            return self.memtable[key]
        
        for sstable_file in self._sstables:
            filepath = os.path.join(self._dir, sstable_file)
            with open(filepath, 'rb') as f:
                for line in f:
                    record = json.loads(line)
                    if record[key] == key:
                        if record['value'] is None:
                            return None
                        return record['value']
        return None   
    
    def _flush(self):
        
        if not self.memtable:
            return
        
        timestamp = int(time.time() * 1_000_000)
        file_name = f"{timestamp}.jsonl"
        file_path = os.path.join(self._dir, file_name)
        
        sorted_items = sorted(self.memtable.items())
        with open (file_path, 'w') as f:
            for key, value in sorted_items:
                json_value = None if value == "$" else value
                record = {'key': key, 'value': json_value}
                f.write(json.dumps(record + '\n'))
                
        print("Flushed memtable to {file_name}")
        
        self._sstables.insert(0, file_name)
        self.memtable.clear        
                 
        
                      
# --- Example Usage ---
if __name__ == '__main__':
    # Use a small threshold to demonstrate flushing
    db = LSMTree("lsm_data", memtable_threshold=4)

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
    db.put("city", "San Francisco")
    db.put("status", "Active")
    db.put("country", "USA")
    
    # This will trigger another flush
    print("\n--- Adding 8th element to trigger flush ---")
    db.put("system", "LSMTree") 
    print("Current SSTables:", db._sstables)
    
    # Demonstrate reads
    print("\n--- Reading data ---")
    print(f"Name: {db.get('name')}") # From the first SSTable
    print(f"City: {db.get('city')}") # From the Memtable (most recent value)
    print(f"System: {db.get('system')}") # From the second SSTable

    # Demonstrate deletion
    print("\n--- Deleting data ---")
    db.delete("language")
    print(f"Language after deletion: {db.get('language')}")

    # Flush the deletion marker to disk
    db.put("a","1")
    db.put("b","2")
    db.put("c","3") # This should trigger the final flush
    print("\n--- Final State ---")
    print("Memtable:", db.memtable)
    print("SSTables:", db._sstables)
    print(f"Language should still be None after flush: {db.get('language')}")
        