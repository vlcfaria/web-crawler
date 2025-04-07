from queue import Queue

class Crawler:
    def __init__(self, seeds, to_crawl, verbose=False):
        self.frontier = Queue()
        self.visited = set()
        self.to_crawl = to_crawl
        self.verbose = verbose

        for s in seeds:
            #TODO canonicalize URL
            self.frontier.put(s)
            self.visited.add(s)
        
