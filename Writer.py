import sys

class Writer:
    """Class responsible for writing WARC format entries, given raw HTML content.
    Pages are split into separate files according to `pages_ratio`.
    Files are then stored as `"target_directory/base_name-xxxx"`"""
    def __init__(self, target_directory: str, base_name ='pages-', pages_ratio=1000):
        
        self.target_directory = target_directory
        self.base_name = base_name
        self.pages_ratio = pages_ratio #Files per page
        if self.pages_ratio <= 0:
            sys.exit("error: pages per file ratio <= 0")
        
        self.file_num = 1
        self.count = 0
        self.cur_file = open(f"{self.target_directory}/{self.base_name}-{self.file_num}.warc", 'w')
    
    def next_file(self):
        'Closes current file, increments file counter and opens new .warc file for future writing'
        self.cur_file.close()
        self.count = 0
        self.file_num += 1

        self.cur_file = open(f"{self.target_directory}/{self.base_name}-{self.file_num}.warc", 'w')
    
    def write(self, html):
        'Takes in raw HTML content and append to current file, swapping if necessary'
        
        #Get next file if needed
        if self.count == self.pages_ratio:
            self.next_file()
        
        #Store!
        
