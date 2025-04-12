import requests
import json
from queue import Queue
from url_normalize import url_normalize
from bs4 import BeautifulSoup
from time import time
from Corpus import Corpus
from urllib.parse import urlparse
from protego import Protego

class Crawler:
    """Base crawler class, responsible for basic crawling and managing similar structures. 
    Uses ordered dequeueing policy."""

    def __init__(self, seeds, to_crawl, verbose=False, default_delay = 0.1):
        self.frontier = Queue()
        self.visited = set()
        self.to_crawl = to_crawl
        self.verbose = verbose
        self.default_delay = default_delay

        self.corpus = Corpus("./output")

        self.host2robots = {}

        self.crawled = 0

        for s in seeds:
            s = url_normalize(s) #Basic normalization, benefit of the doubt that seeds are simple urls
            if (self.url_crawlable(s)):
                self.frontier.put(s)
                self.visited.add(s)
    
    def crawl(self):
        #TODO abide to robots.txt, expand frontier, paralellize, etc
        while not self.frontier.empty() and self.crawled < self.to_crawl:

            url = self.frontier.get()

            try:
                res = requests.get(url, stream=False)
                res.raise_for_status()
            except requests.exceptions.SSLError:
                print(f'error: ssl error on {url}')
                continue #TODO could try adding back to frontier with HTTP?
            except requests.exceptions.ConnectionError:
                print('error: ConnectionError, no internet?')
                continue
            except requests.exceptions.Timeout:
                print(f'error: timeout on {url}')
                continue #TODO maybe setup a retry
            except requests.exceptions.HTTPError as err:
                print(f'error: httpError on {url}')
                continue

            #MIME type must also be html
            mime = res.headers.get('Content-type', '')
            if not mime.startswith('text/html'): continue

            #All ok!
            self.crawled += 1
            soup = BeautifulSoup(res.text, 'html.parser')

            #Store in corpus + print
            self.corpus.write(url, res)

            if self.verbose:
                self.print_request(res.url, soup)
            
            self.process_outlinks(url, soup)

        self.corpus.close()
    
    def process_outlinks(self, url, soup):
        #Expand queue by finding links
        for tag in soup.find_all('a'):
            link = tag.get('href')

            #Skip empty/missing href and hashes
            if link is None or link == '' or link[0] == '#': continue

            try:
                normal = url_normalize(link, default_domain=url, filter_params=True)
                normal = normal.split('#', 1)[0] #Remove hashes # in links too

            except: #Couldnt parse url, probably not an url in the first place...
                continue

            if not self.url_crawlable(normal):
                continue
            
            if normal not in self.visited: #Revisitation policy
                self.frontier.put(normal)
                self.visited.add(normal)
    
    def url_crawlable(self, url: str) -> bool:
        """Checks if url is crawlable given host's robot.txt. Only checks for allow/disallow tags.
        Does not take into account crawl delay, as that is handled by the frontier."""

        parsed = urlparse(url)
        origin = f"{parsed.scheme}://{parsed.netloc}"

        if origin in self.host2robots:
            val = self.host2robots[origin]

            if type(val) == float: return True #robots not found, no link restrictions
            return self.host2robots[origin].can_fetch(url, '')
        
        #Look for robots.txt
        try:
            resp = requests.get(f"{origin}/robots.txt")
        except: #Too much can go wrong here, just assume it doesn't exist
            self.host2robots[origin] = self.default_delay
            return True
        
        self.host2robots[origin] = Protego.parse(resp.text)
        return self.host2robots[origin].can_fetch(url, '')


    def print_request(self, url, soup):
        #Get first 20 words. Very inefficient, but ok due to debugging only
        text = ' '.join(soup.body.text.split()[:20]) if soup.body else 'N/A'
        title = soup.title.text if soup.title else 'N/A'

        obj = {'URL': url, 'Title': title,
               'Text': text, 'Timestamp': int(time())}
        print(json.dumps(obj, indent=1))