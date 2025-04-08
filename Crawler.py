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
        #TODO abide to robots.txt, expand frontier, paralellize, etc
        while not self.frontier.empty():

            url = self.frontier.get()
            res = requests.get(url, stream=False)

            #Response must be OK or similar HTTP response
            if res.status_code // 100 != 2:
                continue

            #MIME type must also be html
            mime = res.headers.get('Content-type', '')
            if not mime.startswith('text/html'): continue

            #All ok!
            soup = BeautifulSoup(res.text, 'html.parser')

            #Store in corpus + print
            self.corpus.write(url, res)

            if self.verbose:
                self.print_request(res.url, soup)
            
            #Expand queue by finding links
            for tag in soup.find_all('a'):
                link = tag.get('href')

                #Skip empty, missing href or hash
                if link is None or link == '' or link[0] == '#': continue

                try: #TODO remove hashes # in links too
                    normal = url_normalize(link, default_domain=url, filter_params=True)
                except: #Couldnt parse url, maybe not an URL? TODO check correctness
                    continue
                if normal not in self.visited: #Not yet visited, add to frontier
                    self.frontier.put(normal)
                    self.visited.add(normal)

        self.corpus.close()

    def print_request(self, url, soup):
        #Get first 20 words. Very inefficient, but ok due to debugging only
        text = ' '.join(soup.body.text.split()[:20]) if soup.body else 'N/A'
        title = soup.title.text if soup.title else 'N/A'

        obj = {'URL': url, 'Title': title,
               'Text': text, 'Timestamp': int(time())}
        print(json.dumps(obj, indent=1))