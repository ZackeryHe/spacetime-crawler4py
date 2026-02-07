import os
import shelve
import time

from collections import deque
from threading import Condition
from urllib.parse import urlparse

from utils import get_logger, get_urlhash, normalize
from scraper import is_valid

class Frontier(object):
    def __init__(self, config, restart):
        self.logger = get_logger("FRONTIER")
        self.config = config

        # pretty simple multithreading implementation
        # we store a q of each domain and the URLS it wants to download and the last time a url was taken from this domain
        # (with thread lock) we iterate through the domains and pop the first one thats after 0.5s elapsed
        # (with thread lock) we add url to queue 
        self.domain_queues = {}       # hostname -> deque of URLs
        self.last_request_time = {}   # hostname -> timestamp of last dispatch
        self.active_workers = 0       # count of workers currently processing

        # Condition variable for thread coordination
        self.url_available = Condition()

        if not os.path.exists(self.config.save_file) and not restart:
            # Save file does not exist, but request to load save.
            self.logger.info(
                f"Did not find save file {self.config.save_file}, "
                f"starting from seed.")
        elif os.path.exists(self.config.save_file) and restart:
            # Save file does exists, but request to start from seed.
            self.logger.info(
                f"Found save file {self.config.save_file}, deleting it.")
            os.remove(self.config.save_file)
        # Load existing save file, or create one if it does not exist.
        self.save = shelve.open(self.config.save_file)
        if restart:
            for url in self.config.seed_urls:
                self.add_url(url)
        else:
            # Set the frontier state with contents of save file.
            self._parse_save_file()
            if not self.save:
                for url in self.config.seed_urls:
                    self.add_url(url)

    def _get_domain(self, url):
        return urlparse(url).netloc

    def _parse_save_file(self):
        ''' This function can be overridden for alternate saving techniques. '''
        total_count = len(self.save)
        tbd_count = 0
        for url, completed in self.save.values():
            if not completed and is_valid(url):
                domain = self._get_domain(url)
                if domain not in self.domain_queues:
                    self.domain_queues[domain] = deque()
                self.domain_queues[domain].append(url)
                tbd_count += 1
        self.logger.info(
            f"Found {tbd_count} urls to be downloaded from {total_count} "
            f"total urls discovered.")

    def get_tbd_url(self):
        with self.url_available:
            while True:
                now = time.time()
                best_url = None
                best_domain = None
                shortest_wait = None

                for domain, queue in self.domain_queues.items():
                    if not queue:
                        continue
                    last_time = self.last_request_time.get(domain, 0)
                    elapsed = now - last_time
                    if elapsed >= self.config.time_delay:
                        # This domain is ready
                        best_domain = domain
                        best_url = queue.popleft()
                        break
                    else:
                        # Track shortest remaining cooldown
                        remaining = self.config.time_delay - elapsed
                        if shortest_wait is None or remaining < shortest_wait:
                            shortest_wait = remaining

                if best_url is not None:
                    self.last_request_time[best_domain] = time.time()
                    self.active_workers += 1
                    # Clean up empty queues
                    if not self.domain_queues[best_domain]:
                        del self.domain_queues[best_domain]
                    return best_url

                # Check if all queues are empty
                has_urls = any(q for q in self.domain_queues.values())

                if not has_urls and self.active_workers == 0:
                    # Crawl is finished
                    return None

                if not has_urls and self.active_workers > 0:
                    # Other workers may add URLs soon
                    self.url_available.wait(timeout=1.0)
                elif shortest_wait is not None:
                    # All domains on cooldown, wait for shortest one
                    self.url_available.wait(timeout=shortest_wait)

    def add_url(self, url):
        url = normalize(url)
        urlhash = get_urlhash(url)
        with self.url_available:
            if urlhash not in self.save:
                self.save[urlhash] = (url, False)
                self.save.sync()
                domain = self._get_domain(url)
                if domain not in self.domain_queues:
                    self.domain_queues[domain] = deque()
                self.domain_queues[domain].append(url)
                self.url_available.notify_all()

    def mark_url_complete(self, url):
        urlhash = get_urlhash(url)
        if urlhash not in self.save:
            # This should not happen.
            self.logger.error(
                f"Completed url {url}, but have not seen it before.")

        with self.url_available:
            self.save[urlhash] = (url, True)
            self.save.sync()
            self.active_workers -= 1
            self.url_available.notify_all()
