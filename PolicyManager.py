from protego import Protego
from urllib.parse import urlparse
import requests

class PolicyManager:
    def __init__(self, default_delay=0.1):
        "Class for holding `robots.txt` info for both the crawler and the frontier."

        self.hosts = {}
        self.default_delay = default_delay

    def get_delay(self, url: str) -> float:
        "Gets crawl-delay from host, or default delay if not inexistent."

        h = self._extract_host(url)

        if h not in self.hosts:
            self._get_rules(h)
        
        val = self.hosts[h].crawl_delay('') if self.hosts[h] != None else None

        return self.default_delay if val == None else val

    def can_fetch(self, url: str) -> bool:
        "Returns if crawling the given url is allowed."

        h = self._extract_host(url)

        if h not in self.hosts:
            self._get_rules(h)

        val = self.hosts[h]

        return True if val == None else val.can_fetch(url, '')

    def _get_rules(self, host: str) -> None:
        "Fetches `robots.txt` from host"

        try:
            resp = requests.get(f"{host}/robots.txt", timeout=1) #Tighter timeout for robots
            resp.raise_for_status()
            self.hosts[host] = Protego.parse(resp.text)

        except requests.RequestException: #Website does not have robots.txt or not responding
            self.hosts[host] = None
        

    def _extract_host(self, url):
        "Extracts the host from the URL. Assumes URL is valid."

        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}"
