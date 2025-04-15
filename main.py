import argparse
import sys
from Crawler import Crawler
import threading

def parse_arguments():
    parser = argparse.ArgumentParser(prog='web-crawler')

    parser.add_argument('-s', type=str, help='path to seeds file', required=True)
    parser.add_argument('-n', type=int, help='target number of webpages to be crawled', required=True)
    parser.add_argument('-d', help='run in debug mode', action='store_true')

    return parser.parse_args()

def parse_seeds(seeds):
    with open(seeds) as f:
        return [line.rstrip() for line in f.readlines()]


if __name__ == '__main__':
    args = parse_arguments()

    if args.n < 0:
        sys.exit("error: number of webpages crawled must be positive")
        
    #Parse seeds
    try:
        seeds = parse_seeds(args.s)
    except FileNotFoundError:
        sys.exit(f"error: file {args.s} not found")
    
    #Call crawler
    NUM_WORKERS = 25
    c = Crawler(seeds, args.n, args.d, NUM_WORKERS)
    threads = [threading.Thread(target=c.crawl, args= (i,)) for i in range(NUM_WORKERS)]

    for t in threads:
        t.start()
    
    for t in threads:
        t.join()