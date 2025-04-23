import requests
import json
from url_normalize import url_normalize
from bs4 import BeautifulSoup
import time
from Corpus import Corpus
from urllib.parse import urlparse, urljoin
from Frontier import Frontier
from PolicyManager import PolicyManager
import re
from threading import Lock

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

#Taken from https://github.com/django/django/blob/main/django/core/validators.py
url_regex = re.compile(
        r'^(?:http)s?://' # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|' #domain...
        r'localhost|' #localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})' # ...or ip
        r'(?::\d+)?' # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)

class Crawler:
    """Base crawler class, responsible for basic crawling and managing similar structures. 
    Uses ordered dequeueing policy."""

    def __init__(self, seeds, to_crawl, verbose=False, num_workers=10, filter_ratio=1000):
        self.policies = PolicyManager()
        #Benefit of the doubt, assume that starting seeds are well-formatted URL
        self.frontier = Frontier(self.policies, num_workers, [url_normalize(s) for s in seeds], filter_ratio * to_crawl)
        self.to_crawl = to_crawl #Number of pages to crawl
        self.verbose = verbose
        self.corpus = Corpus("./output")

        self.crawled = 0
        self.headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36'}

        self.lock = Lock()
        
        #Sessions is better than plain requests.get
        self.sessions = [requests.session() for _ in range(num_workers)]

        #TODO check if this makes it betters
        for s in self.sessions:
            retries = Retry(total=3, backoff_factor=0.3)
            adapter = HTTPAdapter(max_retries=retries)
            s.mount("http://", adapter)
            s.mount("https://", adapter)
    
    def crawl(self, tid):
        fetch_func = (lambda url: self.fetch_url(url, tid))
        while True:
            with self.lock:
                if (self.crawled >= self.to_crawl): break

            res = self.frontier.get(fetch_func) #Assumes crawl-delay is taken into account by frontier
            if res == None: continue #Fetch unsuccesful

            #Handle redirects, by adding the new location back in frontier
            if res.status_code in [301, 302, 307, 308]:
                if 'Location' not in res.headers:
                    continue #Redirect, but no location??
                new_url = res.headers['Location']
                new_url = self.normalize_url(res.url, new_url)
                if new_url != '':
                    self.frontier.put(new_url)
                continue

            #MIME type must also be html
            mime = res.headers.get('Content-type', '')
            if not mime.startswith('text/html'): continue

            #All ok!
            soup = BeautifulSoup(res.text, 'html.parser')

            with self.lock: #One last check before writing
                if self.crawled >= self.to_crawl: break
                self.crawled += 1

            #Store in corpus + print
            self.corpus.write(res.url, res)

            if self.verbose:
                self.print_request(res.url, soup)
            
            self.process_outlinks(res.url, soup)

        self.corpus.close()
            
    def fetch_url(self, url, tid) -> requests.models.Response | None:
        "Fetches a given URL. Returns None if fetch was unsucessful according to `robots.txt` or other errors."

        if not self.policies.can_fetch(url): #Will check robots.txt allow/disallow
            return None
        
        try:
            #Important detail -> disallow redirects
            res = self.sessions[tid].get(url, stream=False, timeout=5, allow_redirects=False, headers=self.headers)
            res.raise_for_status()
        except requests.exceptions.SSLError:
            #print(f'error: ssl error on {url}')
            return None
        except requests.exceptions.ConnectionError as e:
            #print('error: ConnectionError on', url)
            #print(e)
            return None
        except requests.exceptions.Timeout:
            #print(f'error: timeout on {url}')
            return None
        except requests.exceptions.HTTPError as err:
            #print(f'error: httpError on {url}')
            #print(err)
            return None
        
        return res

    def process_outlinks(self, url: str, soup: BeautifulSoup) -> None:
        """Processes outlinks parsed in all <a> tags in parsed structure, while also checking for url malformation and invalid protocols.
        
        Handles malformatted urls, relative urls and protocols, while also performing url normalization. Only HTTP/HTTPS protocols are allowed.
        """

        #Expand queue by finding links
        for tag in soup.find_all('a'):
            link = tag.get('href')

            #Skip empty/missing href and hashes
            if link is None or link == '' or link[0] == '#': continue

            normal = self.normalize_url(url, link)

            if normal != '':
                self.frontier.put(normal) #Frontier will handle visited set
        
    def normalize_url(self, original_url: str, new_url: str) -> str:
        "Normalizes an URL. Handles relative urls and relative protocols. If URL is invalid, returns `''`."

        #Handle relative url + relative protocols
        try:
            new_url = urljoin(original_url, new_url)
        except: #A lot can go wrong here, just skip if needed
            return ''

        #Check this BEFORE normalization, since url_normalize is apparently very cost inneficient
        if re.match(url_regex, new_url) == None:
            return ''
       
        try:
            normal = url_normalize(new_url, filter_params=True)
            normal = normal.split('#', 1)[0] #Remove hashes # in links too
        except: #Couldnt parse url, probably not an url in the first place...
            return ''

        return normal #Sucess


    def print_request(self, url, soup):
        #Get first 20 words. Very inefficient, but ok due to debugging only
        text = ' '.join(soup.body.text.split()[:20]) if soup.body else 'N/A'
        title = soup.title.text if soup.title else 'N/A'

        obj = {'URL': url, 'Title': title,
               'Text': text, 'Timestamp': int(time.time())}
        print(json.dumps(obj, indent=1))