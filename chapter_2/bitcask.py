import os
import struct

class Bitcask:

    def __init__(self, directory):
        self._dir = directory
        
        if not os.path.exists(self._dir):
            os.makedirs(self._dir)
            
        self._active_file = None
        self._key_dir = {}
        # This will hold the file which is currently open in bitcask only the current file is 
        # kept open at any time
        
        # This is a crucial step when starting the database. We need to "re-learn"
        # all the key locations by reading the files that are already on the disk.
        # This populates our _keydir so we know where everything is.
        self._load_keydir() 
    
    """
    Scans all data files on disk to rebuild the in-memory key directory.

    This function is called on startup to restore the database's state. It
    iterates through each data file in chronological order (by sorting the
    filenames) to ensure that data is read as it was written.

    For each record found in a data file, it reads the header to determine the
    size of the key and value. It then reads the key and calculates the
    precise location of the value on disk (which file it's in, the byte
    offset where the value starts, and the size of the value).

    This location information is stored as a "pointer" in the `self._keydir`
    dictionary. If a key is found multiple times across the files (which
    happens when a value is updated), this process correctly overwrites the
    older pointer with the newer one, ensuring the in-memory index always
    points to the most recent version of the value.
    """    
    def _load_keydir(self):
        
        for filename in sorted(os.listdir(self._dir)):
            if filename.endswith('.dat'):
                filepath = os.path.join(self._dir, filename)
                with open(filepath, 'rb') as f:
                    offset = 0
                    while True:
                        # Read header: timestamp(4), key size (4), value_size(4)
                        header = f.read(12)
                        if not header:
                            break
                        #Unpack the header to get the key and value sizes
                        _, key_size, value_size = struct.unpack('>III', header) 
                        
                        key = f.read(key_size).decode('utf-8')
                        
                        self._key_dir[key] = (filepath, offset + 12 + key_size, value_size)
                        f.seek(value_size, 1)
                        offset = f.tell()  
    
    """
    Retrieves the value for a given key.

    Args:
    key (str): The key to retrieve.

    Returns:
    bytes or None: The value associated with the key, or None if not found.
    """
    def get(self, key):
        if key not in self._key_dir:
            return None
        
        filepath, offset, value_size = self._key_dir[key]
        with open(filepath, 'rb') as f:
            f.seek(offset)
            return f.read(value_size)
        
    def put(self, key, value):
        if self._active_file is None:
            self._active_file = self._create_new_file()
            
        key_bytes = key.encode('utf-8')    
        key_size = len(key_bytes)
        value_size = len(value)
        
        timestamp = 0
        header = struct.pack('>III', timestamp, key_size, value_size)
        
        with open(self._active_file, 'ab') as f:
            offset = f.tell()
            f.write(header)
            f.write(key_bytes)
            f.write(value)
            
        self._key_dir[key] = (self._active_file, offset + 12 + key_size, value_size)     
        
    
    def _create_new_file(self):
        
        files = [f for f in os.listdir(self._dir) if f.endswith(".dat")]
        next_id = len(files)
        file_path = os.path.join(self._dir, f"{next_id}.dat")
        return file_path  
    
    def delete(self, key):
        
        if key in self._key_dir:
            self.put(key, b'')
            del self._key_dir[key]
    

# --- Example Usage ---
if __name__ == '__main__':
    db = Bitcask("bitcask_data")

    # Put some values
    db.put("name", b"Alice")
    db.put("city", b"New York")
    db.put("language", b"Python")

    # Get and print values
    print(f"Name: {db.get('name').decode('utf-8')}")
    print(f"City: {db.get('city').decode('utf-8')}")

    # Overwrite a value
    db.put("city", b"San Francisco")
    print(f"Updated City: {db.get('city').decode('utf-8')}")

    # Delete a value
    db.delete("language")
    print(f"Language after deletion: {db.get('language')}")                                      