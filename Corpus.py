import sys
from warcio.warcwriter import WARCWriter
from warcio.statusandheaders import StatusAndHeaders
from io import BytesIO

class Corpus:
    """Class responsible for writing WARC format entries, given request response, storing into a Corpus.

    Pages are split into separate files according to `pages_ratio`.
    Files are then stored as `"target_directory/base_name-xxxx"`"""
    def __init__(self, target_directory: str, base_name ='pages', pages_ratio=1000):
        
        self.target_directory = target_directory
        self.base_name = base_name
        self.pages_ratio = pages_ratio #Files per page
        if self.pages_ratio <= 0:
            sys.exit("error: pages per file ratio <= 0")
        
        self.file_num = 1
        self.count = 0
        self.cur_file = open(f"{self.target_directory}/{self.base_name}-{self.file_num}.gz", 'wb')
        self.writer = WARCWriter(self.cur_file, gzip=True)
    
    def next_file(self):
        'Closes current file, increments file counter and opens new .warc file for future writing'
        self.cur_file.close()
        self.count = 0
        self.file_num += 1

        #Refresh file and writer
        self.cur_file = open(f"{self.target_directory}/{self.base_name}-{self.file_num}.warc", 'wb')
        self.writer = WARCWriter(self.cur_file, gzip=True)
    
    def write(self, url, resp):
        'Takes in raw response content and append to current file, swapping if necessary'
        #Get next file if needed
        if self.count == self.pages_ratio:
            self.next_file()
        
        #Create record
        headers_list = resp.raw.headers.items()
        http_headers = StatusAndHeaders('200 OK', headers_list, protocol='HTTP/1.0')

        #warcio expects a stream, so convert content to stream
        record = self.writer.create_warc_record(url, 'response',
                                                payload=BytesIO(resp.content), http_headers=http_headers)
        
        #Store & increment
        self.writer.write_record(record)
        self.count += 1
    
    def close(self):
        if not self.cur_file.closed:
            self.cur_file.close()