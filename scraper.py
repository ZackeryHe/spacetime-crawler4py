import re
import os
from urllib.parse import urlparse, urljoin, urldefrag
from bs4 import BeautifulSoup


# *.ics.uci.edu, *.cs.uci.edu, *.informatics.uci.edu, *.stat.uci.edu
ALLOWED_DOMAIN_PATTERNS = [
    re.compile(r'^(.+\.)?ics\.uci\.edu$'),
    re.compile(r'^(.+\.)?cs\.uci\.edu$'),
    re.compile(r'^(.+\.)?informatics\.uci\.edu$'),
    re.compile(r'^(.+\.)?stat\.uci\.edu$'),
]


# note we store LOWERCASE only tokens right now
def _load_stop_words():
    stop_words = set()
    stopwords_path = os.path.join(os.path.dirname(__file__), 'stopwords.txt')
    with open(stopwords_path, 'r') as f:
        for line in f:
            for word in line.strip().split():
                stop_words.add(word.lower())
    return stop_words

STOP_WORDS = _load_stop_words()

def scraper(url, resp):
    links = extract_next_links(url, resp)
    return [link for link in links if is_valid(link)]

def extract_next_links(url, resp):
    # Implementation required.
    # url: the URL that was used to get the page
    # resp.url: the actual url of the page
    # resp.status: the status code returned by the server. 200 is OK, you got the page. Other numbers mean that there was some kind of problem.
    # resp.error: when status is not 200, you can check the error here, if needed.
    # resp.raw_response: this is where the page actually is. More specifically, the raw_response has two parts:
    #         resp.raw_response.url: the url, again
    #         resp.raw_response.content: the content of the page!
    # Return a list with the hyperlinks (as strings) scrapped from resp.raw_response.content
    links = []
    
    if resp.status != 200:
        return links
    
    if not resp.raw_response or not resp.raw_response.content:
        return links
    
    try:
        soup = BeautifulSoup(resp.raw_response.content, 'lxml')
        base_url = resp.url if resp.url else url
        
        for anchor in soup.find_all('a', href=True):
            href = anchor['href']
            absolute_url = urljoin(base_url, href)
            url_without_fragment, _ = urldefrag(absolute_url)
            links.append(url_without_fragment)
    except Exception as e:
        pass
    
    return links

def is_valid(url):
    # Decide whether to crawl this url or not. 
    # If you decide to crawl it, return True; otherwise return False.
    # There are already some conditions that return False.
    try:
        parsed = urlparse(url)
        
        if parsed.scheme not in set(["http", "https"]):
            return False
        
        if not parsed.netloc:
            return False
        
        domain = parsed.netloc.lower()
        if not any(pattern.match(domain) for pattern in ALLOWED_DOMAIN_PATTERNS):
            return False
        
        if re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower()):
            return False
        
        return True

    except (TypeError, AttributeError) as e:
        return False
