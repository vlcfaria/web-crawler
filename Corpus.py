import sys
from warcio.warcwriter import WARCWriter
from warcio.statusandheaders import StatusAndHeaders
from io import BytesIO
import requests
import threading

class Corpus:
    '''
    Class responsible for writing WARC format entries, given request response, storing into local storage.
    '''
    def __init__(self, target_directory: str, base_name='pages', pages_ratio=1000):
        '''
        Pages are split into separate files, each one of them with `pages_ratio` WARC entries.
        Files are then stored as `"target_directory/base_name-xxxx.warc.gz"`
        '''

        self.target_directory = target_directory
        self.base_name = base_name
        self.pages_ratio = pages_ratio
        if self.pages_ratio <= 0:
            sys.exit("error: pages per file ratio <= 0")
        
        self.file_num = 1
        self.count = 0
        self.lock = threading.Lock()

        self.cur_file = open(f"{self.target_directory}/{self.base_name}-{self.file_num}.warc.gz", 'wb')
        self.writer = WARCWriter(self.cur_file, gzip=True)
    
    def _next_file(self) -> None:
        'Lock before calling this! Closes current file, increments file counter and opens new .warc file for future writing.'
        self.cur_file.close()
        self.count = 0
        self.file_num += 1

        #Refresh file and writer
        self.cur_file = open(f"{self.target_directory}/{self.base_name}-{self.file_num}.warc.gz", 'wb')
        self.writer = WARCWriter(self.cur_file, gzip=True)
    
    def write(self, url: str, resp: requests.Response) -> None:
        'Takes in raw response content and append to current file, swapping if necessary'
        with self.lock:
            #Get next file if needed
            if self.count == self.pages_ratio:
                self._next_file()
            
            #Create record
            headers_list = resp.raw.headers.items()
            protocol = getattr(resp.raw, 'version_string', 'HTTP/1.1')
            http_headers = StatusAndHeaders(f"{resp.status_code} {resp.reason}", headers_list, protocol=protocol)

            #warcio expects a stream, so convert content to stream
            record = self.writer.create_warc_record(url, 'response',
                                                    payload=BytesIO(resp.content), http_headers=http_headers)
            
            #Store & increment
            self.writer.write_record(record)
            self.count += 1
    
    def close(self) -> None:
        'Closes current file, if not yet closed.'
        with self.lock:
            if not self.cur_file.closed:
                self.cur_file.close()