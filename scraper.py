import re
import os
import sys
from urllib.parse import urlparse, urljoin, urldefrag
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning

import warnings
from utils import get_logger
from utils.duplicate_checker import DuplicateChecker
from utils.analytics import analytics, analytics_lock, maybe_save_analytics, save_analytics
from utils.url_filters import UrlFilter
from utils.content_filters import ContentFilter
from utils.tokenizer import tokenize_string

_logger = get_logger("SCRAPER")
_duplicate_checker = DuplicateChecker(n=1000, similarity_threshold=0.9)
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

# set to -1 for no limit
TESTING_LIMIT = -1

DATASET_THRESHOLD = 0.4
TEXT_RATIO_THRESHOLD = 0.6

# *.ics.uci.edu, *.cs.uci.edu, *.informatics.uci.edu, *.stat.uci.edu
ALLOWED_DOMAIN_PATTERNS = [
    re.compile(r'^(.+\.)?ics\.uci\.edu$'),
    re.compile(r'^(.+\.)?cs\.uci\.edu$'),
    re.compile(r'^(.+\.)?informatics\.uci\.edu$'),
    re.compile(r'^(.+\.)?stat\.uci\.edu$'),
]
BINARY_EXTENSIONS = {
    '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.ico', '.tiff', '.tif',
    '.webp', '.avif', '.svg', '.psd',
    '.mp3', '.mp4', '.wav', '.avi', '.mov', '.mkv', '.ogg', '.ogv',
    '.webm', '.flv', '.wmv', '.wma', '.mid', '.m4v', '.mpeg', '.ram',
    '.pdf', '.doc', '.docx', '.ppt', '.pptx', '.xls', '.xlsx',
    '.zip', '.rar', '.gz', '.tar', '.bz2', '.7z', '.iso',
    '.exe', '.dmg', '.msi', '.bin', '.dll', '.jar',
    '.woff', '.woff2', '.ttf', '.otf', '.eot',
    '.css', '.js',
    '.lif', '.rle', '.pov',
    '.ps', '.eps', '.tex', '.names', '.data', '.dat', '.cnf', '.tgz', '.sha1',
    '.thmx', '.mso', '.arff', '.rm', '.smil', '.img', '.apk', '.war', '.sql', '.db', '.bak',
    '.epub', '.rtf', '.csv', '.swf', '.mol', '.java', '.can', '.untetra', '.bib', '.py', '.c', '.h', '.ff'
}
_url_filter = UrlFilter(ALLOWED_DOMAIN_PATTERNS, BINARY_EXTENSIONS, dataset_threshold=DATASET_THRESHOLD)
_content_filter = ContentFilter(text_ratio_threshold=TEXT_RATIO_THRESHOLD)


def _load_stop_words():
    stop_words = set()
    stopwords_path = os.path.join(os.path.dirname(__file__), 'stopwords.txt')
    with open(stopwords_path, 'r') as f:
        for line in f:
            for word in line.strip().split():
                stop_words.add(word.lower())
    return stop_words

STOP_WORDS = _load_stop_words()

MIN_CONTENT_SIZE = 100
MAX_CONTENT_SIZE = 5_000_000

def scraper(url, resp):
    if TESTING_LIMIT >= 0 and analytics['pages_processed'] >= TESTING_LIMIT:
        if not analytics['report_generated']:
            generate_report()
            analytics['report_generated'] = True
        raise Exception(f"Reached testing limit of {TESTING_LIMIT} pages. Report generated. Stopping crawler.")

    links = extract_next_links(url, resp)
    valid = []
    for link in links:
        if is_valid(link):
            valid.append(link)
        else:
            with analytics_lock:
                analytics['skipped_url_filter'] += 1
    return valid

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
        with analytics_lock:
            analytics['skipped_not_200'] += 1
        return links
    
    if not resp.raw_response or not resp.raw_response.content:
        with analytics_lock:
            analytics['skipped_empty_or_size'] += 1
        return links

    content_len = len(resp.raw_response.content)
    if content_len < MIN_CONTENT_SIZE or content_len > MAX_CONTENT_SIZE:
        with analytics_lock:
            analytics['skipped_empty_or_size'] += 1
        return links

    try:
        soup = BeautifulSoup(resp.raw_response.content, 'lxml')
        base_url = resp.url if resp.url else url
        
        # outdated: there are 2 places where we add skip conditions
        # - is the url good?
        # - is the text content likely good?
        # we will put the text content checking ones right here before we call process_page_analytics

        # ...actually, I think checks here are low value because we have already downloaded the page
        # efforts should be focused all on url detection

        text = soup.get_text()
        if _content_filter.should_skip(text):
            with analytics_lock:
                analytics['skipped_empty_or_size'] += 1
            return links
        if _duplicate_checker.is_duplicate(text):
            with analytics_lock:
                analytics['skipped_duplicate'] += 1
            return links

        url_without_fragment, _ = urldefrag(base_url)
        process_page_analytics(url_without_fragment, soup)

        for anchor in soup.find_all('a', href=True):
            href = anchor['href']
            absolute_url = urljoin(base_url, href)
            url_without_fragment, _ = urldefrag(absolute_url)
            links.append(url_without_fragment)
    except Exception:
        with analytics_lock:
            analytics['skipped_error'] += 1

    return links

def process_page_analytics(url, soup):
    for script_or_style in soup(['script', 'style']):
        script_or_style.decompose()

    text = soup.get_text()

    # extract alphanumeric words
    tokens = tokenize_string(text)

    # filter out stop words and 1-character words
    words = [token for token in tokens if token not in STOP_WORDS and len(token) > 1]

    word_count = len(words)
    local_freqs = {}
    for word in words:
        local_freqs[word] = local_freqs.get(word, 0) + 1

    parsed = urlparse(url)
    subdomain = parsed.netloc.lower()

    # update analytics at once under lock
    with analytics_lock:
        analytics['pages_processed'] += 1
        analytics['unique_urls'].add(url)
        for word, count in local_freqs.items():
            analytics['word_frequencies'][word] = analytics['word_frequencies'].get(word, 0) + count
        if word_count > analytics['longest_page']['word_count']:
            analytics['longest_page']['url'] = url
            analytics['longest_page']['word_count'] = word_count
        if subdomain not in analytics['subdomains']:
            analytics['subdomains'][subdomain] = set()
        analytics['subdomains'][subdomain].add(url)

    maybe_save_analytics()

def is_valid(url):
    if TESTING_LIMIT >= 0 and analytics['pages_processed'] >= TESTING_LIMIT:
        return False
    return _url_filter.is_valid(url)

def generate_report(filename="report.txt"):
    unique_urls = analytics['unique_urls']
    longest = analytics['longest_page']
    word_freqs = analytics['word_frequencies']
    subdomains = analytics['subdomains']
    top_50 = sorted(word_freqs.items(), key=lambda x: x[1], reverse=True)[:50]
    ics_subdomains = {
        sub: urls for sub, urls in subdomains.items()
        if sub.endswith('.uci.edu')
    }

    with open(filename, 'w') as f:
        f.write(f"Q1: Unique Pages\n")
        f.write(f"{len(unique_urls)}\n\n")

        f.write(f"Q2: Longest Page (by word count)\n")
        f.write(f"{longest['url']}\n")
        f.write(f"{longest['word_count']} words\n\n")

        f.write(f"Q3: 50 Most Common Words\n")
        for rank, (word, count) in enumerate(top_50, 1):
            f.write(f"{rank}. {word} - {count}\n")
        f.write("\n")

        f.write(f"Q4: Subdomains ({len(ics_subdomains)} found)\n")
        for sub in sorted(ics_subdomains.keys()):
            f.write(f"{sub}, {len(ics_subdomains[sub])}\n")

    save_analytics()
    _logger.info(f"Report saved to {filename}")
