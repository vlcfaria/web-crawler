import math
import hashlib

class BloomFilter:
    '''Class representing a Bloom Filter, a probabilistic set-like structure which saves memory by not storing items themselves,
    but hashing the items and storing them int a bitarray'''

    def __init__(self, num_items: int, epsilon: float=0.01):
        #Number of hash functions
        self.k = int(math.ceil(-math.log(epsilon) / math.log(2)))

        #Size of bitset
        self.size = int(math.ceil(-(num_items * math.log(epsilon)) / (math.log(2)**2)))

        self.bitset = bytearray((self.size + 7) // 8)

        #Good practice would be to parameterize this
        self.hash_func = hashlib.sha512

    def add(self, s:str):
        '''Adds `s` into bloom filter'''

        h1, h2 = self._get_h1_h2(s)
        hsh = h1
        for _ in range(self.k):
            self._set_bit(hsh)
            #Apply Kirsch-Mitzenmacher-Optimization hi = h1 + ih2 mod m
            #Should work OK even if self.size is not a prime?
            hsh = (hsh + h2) % self.size

    def check(self, s:str):
        '''Checks if `s` is in bloom filter'''
        h1, h2 = self._get_h1_h2(s)
        hsh = h1
        for _ in range(self.k):
            if not self._check_bit(hsh): return False
            hsh = (hsh + h2) % self.size
        
        return True

    def _get_h1_h2(self, s:str):
        '''Gets hashes `h1` and `h2`, which can determine all k hashes (Kirsch-Mitzenmacher optimization)'''
        full = self.hash_func(s.encode()).hexdigest()

        #Splits the SHA512 in half
        h1 = int(full[:64], 16) % self.size
        h2 = int(full[64:], 16) % self.size

        return h1, h2
    
    def _set_bit(self, b: int):
        'Sets bit `b` in bitset'
        self.bitset[b // 8] |= (1 << (b % 8))
    
    def _check_bit(self, b: int):
        'Checks bit `b` in bitset'
        return (self.bitset[b // 8] >> (b % 8)) & 1