import math
import hashlib

class BloomFilter:
    def __init__(self, num_items, epsilon=0.01, hash_func = hashlib.sha256):
        #Number of hash functions
        self.k = int(math.ceil(-math.log(epsilon) / math.log(2)))

        #Size of bitset
        self.size = int(math.ceil(-(num_items * math.log(epsilon)) / (math.log(2)**2)))

        self.bitset = bytearray((self.size + 7) // 8)

        self.hash_func = hash_func

    def add(self, s:str):
        '''Adds `s` into bloom filter'''

        h1, h2 = self._get_h1_h2(s)
        hsh = h1
        for _ in range(self.k):
            self._set_bit(hsh)
            #Apply Kirsch-Mitzenmacher-Optimization hi = h1 + ih2 mod m
            #Should work even if self.size is not a prime?
            hsh = (hsh + h2) % self.size

    def check(self, s:str):
        '''Checks if `s` is in bloom filter'''
        h1, h2 = self._get_h1_h2(s)
        hsh = h1
        for _ in range(self.k):
            if self._check_bit(hsh): return True
            hsh = (hsh + h2) % self.size
        
        return False

    def _get_h1_h2(self, s:str):
        '''Gets hashes `h1` and `h2`, which can determine all k hashes (Kirsch-Mitzenmacher)'''
        h1 = self._hash_to_int(s.encode())
        h2 = self._hash_to_int(('salt123' + s).encode()) #Good enough for bloom filtering...

        return h1, h2
    
    def _hash_to_int(self, s:bytes):
        '''Hashes `s` into an integer, given the hash function with structure similar to `hashlib`'s'''

        return int(self.hash_func(s).hexdigest(), 16) % self.size

    def _set_bit(self, b: int):
        'Sets bit `b` in bitset'
        self.bitset[b // 8] |= (1 << (b % 8))
    
    def _check_bit(self, b: int):
        'Checks bit `b` in bitset'
        return (self.bitset[b // 8] >> (b % 8)) & 1