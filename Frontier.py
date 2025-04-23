from queue import Queue, PriorityQueue
from BloomFilter import BloomFilter
import time
from urllib.parse import urlparse
from PolicyManager import PolicyManager
import threading

class Frontier:
    """Mercator style URL frontier."""
    def __init__(self, policies: PolicyManager, num_workers, starting, filter_size, filter_error=.01):
        self.front = Queue() #Front is a simple queue, no prioritization yet
        for url in starting:
            self.front.put(url)

        #Mercator recommendation #back_queues = 3 * crawler threads
        self.back = [Queue() for _ in range(3*num_workers)]

        #These 2 structures don't need locks, only handled by scheduler (single thread)
        self.inactive_back = set(range(len(self.back))) #Track inactive back queues
        self.domain_map = {} #Maps domain -> back queue

        self.heap = PriorityQueue() #Maintain heap for politeness

        self.visited_lock = threading.Lock()
        self.visited = BloomFilter(filter_size, filter_error)

        self.policies = policies

        self.hinted_empty_lock = threading.Lock()
        self.hinted_empty = set() #Hints at scheduler back queues that may be empty
        self.has_empty = threading.Event()

        #Start scheduler
        self.scheduler = threading.Thread(target=self._scheduler_loop, daemon=True)
        self.scheduler.start()

    def get(self, fetch_func):
        "Grabs an URL from the frontier, calls `fetch_func`, handles structures and returns original `fetch_func` returned value"
        
        #Nothing on heap means: nothing on back queues OR less back queues filled than threads (usually at start)
        allowed_time, back_idx = self.heap.get() #Blocking
        url = self.back[back_idx].get_nowait() #Guaranteed to be there, no waiting

        #Enforce politeness. Should only realistically happen at beggining (few domains)
        now = time.time()
        if now < allowed_time:
            time.sleep(allowed_time - now)
        
        #Fetch!
        ans = fetch_func(url)

        #This MIGHT be empty, but the scheduler can also be inserting data in the meantime
        #The only thread that can confirm this as empty is the scheduler
        #TODO maybe rework this by inserting into heap anyway, and trying get_nowait with try catch?
        empty = self.back[back_idx].qsize() == 0

        if empty: #Hint to the scheduler that this might be empty, he will handle it
            domain = self._url_to_domain(url)
            with self.hinted_empty_lock:
                self.hinted_empty.add(domain)
            self.has_empty.set()
        else: #Just re-add
            self.heap.put_nowait((time.time() + self.policies.get_delay(url), back_idx))

        return ans

    def put(self, url: str) -> None:
        "Takes in an url to be put into frontier, if not yet seen."

        with self.visited_lock:
            if self.visited.check(url):
                return
            
            self.visited.add(url)

        #Put in front even if there's inactive at back, scheduler will handle.
        self.front.put(url)

    def _scheduler_loop(self) -> None:
        "Manages scheduling of back queues and front queues, handling required structures."

        while(True):
            self.has_empty.clear()

            with self.hinted_empty_lock:
                while self.hinted_empty:
                    domain = self.hinted_empty.pop()
                    if self.back[self.domain_map[domain]].qsize() == 0: #Really is empty
                        self.inactive_back.add(self.domain_map[domain])
                        del self.domain_map[domain]
                    else:
                        delay = self.policies.get_delay(domain)
                        self.heap.put((time.time()+delay, self.domain_map[domain]))
            
            
            #Check if we need to fill, either by empty heap or new empty entry        
            while self.front.qsize() != 0 and len(self.inactive_back) != 0:
                url = self.front.get()
                domain = self._url_to_domain(url)

                if domain in self.domain_map: #Already in a back queue
                    self.back[self.domain_map[domain]].put_nowait(url)
                    #Preventing future work: check if this was called for inactive_back
                    add = False
                    with self.hinted_empty_lock:
                        if domain in self.hinted_empty:
                            add = True
                            self.hinted_empty.remove(domain)
                    if add:
                        self.heap.put((time.time() + self.policies.get_delay(domain), self.domain_map[domain]))

                else: #Allocate an empty back queue
                    idx = self.inactive_back.pop()
                    #Put in actual queue + register on map & heap
                    self.back[idx].put(url)
                    self.domain_map[domain] = idx

                    #Add delay just in case
                    delay = self.policies.get_delay(domain)
                    self.heap.put_nowait((time.time() + delay, idx))
        
            if len(self.inactive_back) == len(self.back): #Everything is inactive, nothing will call empty
                time.sleep(.1)
            else:
                self.has_empty.wait()

            
    def _url_to_domain(self, url: str) -> str:
        'Gets the domain from an URL'

        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}"
    