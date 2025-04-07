import requests
import json
from queue import Queue
from url_normalize import url_normalize
from bs4 import BeautifulSoup
from time import time
from Corpus import Corpus

class Crawler:
    """Base crawler class, responsible for basic crawling and managing similar structures. 
    Uses ordered dequeueing policy."""

    def __init__(self, seeds, to_crawl, verbose=False):
        self.frontier = Queue()
        self.visited = set()
        self.to_crawl = to_crawl
        self.verbose = verbose
        self.corpus = Corpus("./output")

        for s in seeds:
            s = url_normalize(s) #Basic normalization
            self.frontier.put(s)
            self.visited.add(s)
    
    def crawl(self):
        while not self.frontier.empty():

            url = self.frontier.get()
            res = requests.get(url, stream=False)

            #Response must be OK or similar HTTP response
            if res.status_code // 100 != 2:
                continue

            #MIME type must also be html
            mime = res.headers.get('Content-type', '')
            if not mime.startswith('text/html'): continue

            #Store in corpus
            self.corpus.write(url, res)
            
            #All ok!
            if self.verbose:
                self.print_request(res)

        self.corpus.close()

    def print_request(self, res: requests.Response):
        soup = BeautifulSoup(res.text, 'html.parser')
        
        #Get first 20 words
        text = ' '.join(soup.body.text.split()[:20])

        obj = {'URL': res.url, 'Title': soup.title.text,
               'Text': text, 'Timestamp': int(time())}
        print(json.dumps(obj, indent=1))