from queue import Queue
import heapq
import time
from urllib.parse import urlparse
from PolicyManager import PolicyManager
from threading import Lock


class Frontier:
    """Mercator style URL frontier."""
    def __init__(self, policies: PolicyManager, num_workers):
        self.front = Queue() #Front is a simple queue, no prioritization yet
        
        #Mercator recommendation #back_queues = 3 * crawler threads
        self.back = [Queue() for _ in range(3*num_workers)]
        self.inactive_back = set(range(len(self.back))) #Track inactive back queues

        self.domain_map = {} #Maps domain -> back queue
        self.heap = [] #Maintain heap for politeness

        self.policies = policies
        self.visited = set()

        self.lock = Lock()


    def get(self) -> str:
        "Fetches an URL from the frontier"
        
        with self.lock:
            #If nothing on the heap/back queues, populate back
            if not self.heap:
                self._populate_back()
            
            #TODO avoid loops here when all links end?
            if not self.heap: #If STILL nothing on heap, wait a bit so others populate it again
                return ''
            
            allowed_time, back_idx = heapq.heappop(self.heap)
            url = self.back[back_idx].get()

            empty = False
            if self.back[back_idx].qsize() == 0:
                empty = True
                #Delete from map + populate!
                #TODO move this into a separate worker when multithreading
                domain = self._url_to_domain(url)
                del self.domain_map[domain]
                self.inactive_back.add(back_idx)
                self._populate_back()
    
        #Done managing the structure
        #Enforce politeness. Should only realistically happen at beggining (few domains)
        now = time.time()
        if now < allowed_time:
            time.sleep(allowed_time - now)
        
        #Determine new fetching time, if host didnt change
        #TODO separating these two locks might break by doing a pop on empty heap maybe?
        with self.lock:
            if not empty:
                heapq.heappush(self.heap, (time.time() + self.policies.get_delay(url), back_idx))

        return url

    def put(self, url: str) -> None:
        "Takes in an url to be put into frontier, if not yet seen."

        with self.lock:
            if url in self.visited:
                return
            
            self.visited.add(url)

            #Put in front even if there's inactive at back. Eventually _populate_back will get called
            self.front.put(url)

    def _populate_back(self) -> None:
        "Call lock before calling! Fills back queues with front queue, until either back queues are full or front is empty"

        while (not self.front.qsize() == 0) and len(self.inactive_back) != 0:
            url = self.front.get()
            domain = self._url_to_domain(url)

            if domain in self.domain_map: #Already in a back queue
                self.back[self.domain_map[domain]].put(url)
            else: #Allocate an empty back queue
                idx = self.inactive_back.pop()
                #Put in actual queue + register on map & heap
                self.back[idx].put(url)
                self.domain_map[domain] = idx

                #Add delay just in case
                delay = self.policies.get_delay(domain)
                heapq.heappush(self.heap, (time.time() + delay, idx))
            
    def _url_to_domain(self, url: str) -> str:
        'Gets the domain from an URL'

        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}"
    