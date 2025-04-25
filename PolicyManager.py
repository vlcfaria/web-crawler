from protego import Protego
from urllib.parse import urlparse
import requests
from threading import Lock
from collections import OrderedDict

class PolicyManager:
    '''
    Synchronized class that manages policy data specified in `robots.txt` such as crawl-delay and allowed/disallowed pages.
    To avoid excess memory usage and eventually update old `robots.txt` files, this class operates on a cache.
    '''

    def __init__(self, cache_size:int=1000, default_delay:float=0.1):
        self.cache = OrderedDict() #Caches hosts' robots.txt
        self.cache_size = cache_size

        self.default_delay = default_delay

        self.lock = Lock()

    def get_delay(self, url: str) -> float:
        '''Gets crawl-delay from host, or default delay if not inexistent.'''

        h = self._extract_host(url)

        with self.lock:
            if h not in self.cache: #miss
                self._get_rules(h)
            else:
                self._touch(h)
        
            val = self.cache[h].crawl_delay('') if self.cache[h] else None

        return self.default_delay if val == None else val

    def can_fetch(self, url: str) -> bool:
        '''Returns if crawling the given url is allowed.'''

        h = self._extract_host(url)

        with self.lock:
            if h not in self.cache: #miss
                self._get_rules(h)
            else:
                self._touch(h)

            val = self.cache[h]

        return True if val == None else val.can_fetch(url, '')

    def _get_rules(self, host: str) -> None:
        '''Acquire lock before calling!! Fetches `robots.txt` from host'''

        if len(self.cache) >= self.cache_size: # remove LRU cache
            self.cache.popitem(last=False)

        try:
            resp = requests.get(f"{host}/robots.txt", timeout=1) #Tighter timeout for robots
            resp.raise_for_status()
            self.cache[host] = Protego.parse(resp.text)

        except requests.RequestException: #Website does not have robots.txt or not responding
            self.cache[host] = None
    
    def _touch(self, host: str) -> None:
        '''Move a host to end, indicating it was used'''

        self.cache.move_to_end(host)

    def _extract_host(self, url: str) -> str:
        '''Extracts the host from the URL. Assumes URL is valid.'''

        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}"