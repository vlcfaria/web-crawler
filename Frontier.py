from queue import Queue, PriorityQueue, Empty, Full
from BloomFilter import BloomFilter
import time
from urllib.parse import urlparse
from PolicyManager import PolicyManager
import threading

class Frontier:
    '''
    Class representing a mercator style URL frontier.
    
    Since no priorization is needed, only one Front Queue is used, and the number of Back Queues is proportional to `num_workers`.
    This structure also fires a daemon thread, responsible for managing the front/back queues and the heap when needed.

    For more details about the mercator style URL frontier: `https://nlp.stanford.edu/IR-book/html/htmledition/the-url-frontier-1.html`
    '''
    def __init__(self, policies: PolicyManager, num_workers, starting, filter_size, filter_error=.01):
        self.front = Queue() #Front is a simple queue, no prioritization yet
        for url in starting:
            self.front.put(url)

        #Mercator recommendation #back_queues = 3 * crawler threads
        self.back = [Queue() for _ in range(3*num_workers)]

        #These 2 structures don't need locks, only handled by scheduler (single thread)
        self.inactive_back = set(range(len(self.back))) #Track inactive back queues
        self.domain_map = {} #Maps domain -> back queue, and back queue -> domain (Two way map)

        self.heap = PriorityQueue() #Maintain heap for politeness

        self.visited_lock = threading.Lock()
        self.visited = BloomFilter(filter_size, filter_error)

        self.policies = policies

        self.hinted_empty_lock = threading.Lock()
        self.hinted_empty = set() #Hints at scheduler back queues indexes that may be empty
        self.has_empty = threading.Event()

        #Start scheduler
        self.scheduler = threading.Thread(target=self._scheduler_loop, daemon=True)
        self.scheduler.start()

    def get(self, fetch_func):
        '''Grabs an URL from the frontier, calls `fetch_func`, handles structures and returns original `fetch_func` returned value'''
        
        #Nothing on heap means: nothing on back queues OR less back queues filled than threads (usually at start)
        try:
            allowed_time, back_idx = self.heap.get(timeout=60) #Blocking
        except Empty:
            return None

        try: #Got value from heap, try extracting from back queue
            url = self.back[back_idx].get_nowait()
        except Empty: #Warn scheduler that this MIGHT be empty -> scheduler can be putting values here as we speak
            with self.hinted_empty_lock:
                self.hinted_empty.add(back_idx)
            self.has_empty.set()
            return None

        #Enforce politeness.
        now = time.time()
        if now < allowed_time:
            time.sleep(allowed_time - now)
        
        #Fetch!
        ans = fetch_func(url)

        #Re-add. If back queue is empty, it will be caught in an exception above
        self.heap.put_nowait((time.time() + self.policies.get_delay(url), back_idx))

        return ans

    def put(self, url: str) -> None:
        '''Takes in an url, which is to be put into frontier, if not yet seen.'''

        with self.visited_lock:
            #Not seen before
            if self.visited.check(url):
                return
            
            self.visited.add(url)

        #Put in front even if there's inactive at back, scheduler will handle.
        self.front.put(url)

    def _scheduler_loop(self) -> None:
        '''
        Manages scheduling of back queues and front queues, handling required structures. 
        Avoids busy wait only when all back queues are filled.

        Also responsible for refilling empty back queues hinted by the workers, and re-adding "lost" back queues back into the heap.

        This function should not be called by multiple threads, or by an external object.
        '''

        while(True):
            self.has_empty.clear()

            with self.hinted_empty_lock: #Check queues hinted as empty
                while self.hinted_empty:
                    idx = self.hinted_empty.pop()
                    if self.back[idx].qsize() == 0: #Really is empty
                        self.inactive_back.add(idx)

                        domain = self.domain_map[idx]
                        del self.domain_map[idx] #idx -> domain
                        del self.domain_map[domain] #domain -> idx
            
                    else: #False alarm, readd into heap
                        delay = self.policies.get_delay(self.domain_map[idx])
                        self.heap.put((time.time()+delay, idx))
            
            
            #Check if we need to fill, either by empty heap or new empty entry        
            while self.front.qsize() != 0 and len(self.inactive_back) != 0:
                url = self.front.get()
                domain = self._url_to_domain(url)

                if domain in self.domain_map: #Already in a back queue
                    self.back[self.domain_map[domain]].put_nowait(url)

                    #Preventing future work: check if this was called for inactive_back
                    add = False
                    with self.hinted_empty_lock:
                        if self.domain_map[domain] in self.hinted_empty:
                            add = True
                            self.hinted_empty.remove(self.domain_map[domain]) #Remove the index
                    if add:
                        self.heap.put((time.time(), self.domain_map[domain]))

                else: #Allocate an empty back queue
                    idx = self.inactive_back.pop()
                    #Put in actual queue + register on map & heap
                    self.back[idx].put(url)
                    self.domain_map[domain] = idx
                    self.domain_map[idx] = domain

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
    