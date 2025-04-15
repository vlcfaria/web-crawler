from queue import Queue
import heapq
import time
from urllib.parse import urlparse
from PolicyManager import PolicyManager
import threading
from enum import Enum

class Signal(Enum):
    EMPTY = 1
    FILL = 2


class Frontier:
    """Mercator style URL frontier."""
    def __init__(self, policies: PolicyManager, num_workers):
        self.front = Queue() #Front is a simple queue, no prioritization yet
        #Mercator recommendation #back_queues = 3 * crawler threads
        self.back = [Queue() for _ in range(3*num_workers)]
        self.scheduler_notif = Queue()

        #These 2 structures don't need locks, only handled by scheduler (single thread)
        self.inactive_back = set(range(len(self.back))) #Track inactive back queues
        self.domain_map = {} #Maps domain -> back queue


        self.heap_lock = threading.Lock()
        self.heap = [] #Maintain heap for politeness

        self.visited_lock = threading.Lock()
        self.visited = set()

        self.policies = policies

        #Start scheduler
        self.scheduler = threading.Thread(target=self._scheduler_loop, daemon=True)
        self.scheduler.start()

    def get(self) -> str:
        "Fetches an URL from the frontier"
        
        #Nothing on heap means: nothing on back queues OR less back queues filled than threads (usually at start)
        if not self.heap:
            self.scheduler_notif.put((Signal.FILL, '')) #Call scheduler
        
        #TODO avoid loops here when all links end?
        if not self.heap: #If STILL nothing on heap, wait a bit so others populate it again
            return ''
        
        allowed_time, back_idx = heapq.heappop(self.heap)
        url = self.back[back_idx].get()

        empty = False
        if self.back[back_idx].qsize() == 0:
            empty = True
            domain = self._url_to_domain(url)
            self.scheduler_notif.put((Signal.EMPTY, domain))
    
        #Enforce politeness. Should only realistically happen at beggining (few domains)
        now = time.time()
        if now < allowed_time:
            time.sleep(allowed_time - now)
        
        #Determine new fetching time, if host didnt change
        with self.heap_lock:
            if not empty:
                heapq.heappush(self.heap, (time.time() + self.policies.get_delay(url), back_idx))

        return url

    def put(self, url: str) -> None:
        "Takes in an url to be put into frontier, if not yet seen."

        with self.visited_lock:
            if url in self.visited:
                return
            
            self.visited.add(url)

        #Put in front even if there's inactive at back. Eventually _populate_back will get called
        self.front.put(url)

    def _scheduler_loop(self) -> None:
        "Manages scheduling of back queues and front queues, handling required structures."

        while(True):
            #Wait for requests
            signal, domain = self.scheduler_notif.get()

            if signal == Signal.EMPTY: #Have to delete from map and add to inactive first
                self.inactive_back.add(self.domain_map[domain])
                del self.domain_map[domain]

            #Now do regular work: fill back queues!
            while self.front.qsize() != 0 and len(self.inactive_back) != 0:
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
                    with self.heap_lock:
                        heapq.heappush(self.heap, (time.time() + delay, idx))
            
    def _url_to_domain(self, url: str) -> str:
        'Gets the domain from an URL'

        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}"
    