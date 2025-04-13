from queue import Queue
import heapq
import time
from urllib.parse import urlparse
from PolicyManager import PolicyManager


class Frontier:
    """Mercator style URL frontier."""
    def __init__(self, policies: PolicyManager):
        NUM_WORKERS = 2 #TODO make this variable

        self.front = Queue() #Front is a simple queue, no prioritization yet
        
        #Mercator recommendation #back_queus = 3 * crawler threads
        self.back = [Queue() for _ in range(3*NUM_WORKERS)]
        self.inactive_back = set(range(len(self.back))) #Track inactive back queues

        self.domain_map = {} #Maps domain -> back queue
        self.heap = [] #Maintain heap for politeness
        self.count = 0 #Makes heap stable

        self.policies = policies
        self.visited = set()


    def get(self) -> str:
        "Fetches an URL from the frontier"

        allowed_time, _, back_idx = heapq.heappop(self.heap)
        url = self.back[back_idx].get()

        empty = False
        if self.back[back_idx].empty():
            empty = True
            #TODO call worker to fill back queues
    
        #Done managing the structure
        #Enforce politeness. Should only happen at beggining
        now = time.time()
        if now < allowed_time:
            time.sleep(allowed_time - now)
        
        #Determine new fetching time, if host didnt change
        if not empty:
            heapq.heappush(self.heap, (time.time() + self.policies.get_delay(url), self.count, back_idx))
            self.count += 1

        return url

    def put(self, url: str) -> None:
        "Takes in an url to be put into frontier, if not yet seen."

        if url in self.visited():
            return
        
        self.visited.add(url)

        #If there is an inactive back queue, pass through
        if self.inactive_back:
            pass #TODO add to back
        else:
            self.front.put(url)

    def _url_to_domain(self, url: str) -> str:
        'Gets the domain from an URL'

        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}"
    