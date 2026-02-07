import re
import os
from urllib.parse import urlparse, urljoin, urldefrag
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning

# Beautiful soup sent a warning and said you can ignore it with the following:
import warnings
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

# set to -1 for no limit
TESTING_LIMIT = -1

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

analytics = {
    'unique_urls': set(),           # Set of defragmented URLs for unique page count
    'longest_page': {               # Track longest page by word count
        'url': None,
        'word_count': 0
    },
    'word_frequencies': {},         # Dict of word -> count (excluding stop words)
    'subdomains': {},               # Dict of subdomain -> set of URLs
    'pages_processed': 0,           # Counter for testing limit
    'report_generated': False       # Flag to ensure report is only generated once
}

def _is_calendar_path(parsed_url):
    path = parsed_url.path.lower()
    segments = [seg for seg in path.split('/') if seg]

    # check for YYYY/MM/DD or YYYY/MM patterns
    for i in range(len(segments) - 1):
        # check if segment looks like a year (within 1900-2099)
        if segments[i].isdigit() and len(segments[i]) == 4:
            year = int(segments[i])
            if 1900 <= year <= 2099:
                # check if next segment is a month (01-12)
                if i + 1 < len(segments) and segments[i+1].isdigit():
                    month = int(segments[i+1])
                    if 1 <= month <= 12:
                        return True

    # check for YYYY-MM-DD pattern
    if re.search(r'\b\d{4}-\d{2}-\d{2}\b', path):
        return True

    # check for date parameters in query
    if parsed_url.query:
        query_lower = parsed_url.query.lower()
        date_params = ['date', 'year', 'month', 'day', 'time', 'timestamp', 'ical']
        if any(param in query_lower for param in date_params):
            return True

    return False

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
}

def _has_file_extension_in_query(parsed_url):
    if not parsed_url.query:
        return False
    query_lower = parsed_url.query.lower()

    # check for media-serving actions
    media_actions = ['do=media', 'action=download', 'action=export']
    if any(action in query_lower for action in media_actions):
        return True
    
    # check if any query param value ends with a binary extension
    for ext in BINARY_EXTENSIONS:
        if ext in query_lower:
            return True
    return False

def _is_wiki_trap(parsed_url):
    if 'doku.php' in parsed_url.path.lower() and parsed_url.query:
        query_lower = parsed_url.query.lower()
        wiki_traps = ['do=', 'idx=', 'sectok=']
        if any(trap in query_lower for trap in wiki_traps):
            return True
    return False

def scraper(url, resp):
    if TESTING_LIMIT >= 0 and analytics['pages_processed'] >= TESTING_LIMIT:
        if not analytics['report_generated']:
            generate_report()
            analytics['report_generated'] = True
        raise Exception(f"Reached testing limit of {TESTING_LIMIT} pages. Report generated. Stopping crawler.")

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
        
        # there are 2 places where we add skip conditions
        # - is the url good?
        # - is the text content likely good?
        # we will put the text content checking ones right here before we call process_page_analytics

        # is page empty
        text = soup.get_text()
        if not text.strip():
            return links

        # process
        # consider throwing error if we do logic in process_page_analytics
        # and we decide ignoring it. or just check before calling the function
        # throwing error might be more efficient than processing text 2+ times
        # even though its kinda nasty
        # try:
        url_without_fragment, _ = urldefrag(base_url)
        process_page_analytics(url_without_fragment, soup)

        for anchor in soup.find_all('a', href=True):
            href = anchor['href']
            absolute_url = urljoin(base_url, href)
            url_without_fragment, _ = urldefrag(absolute_url)
            links.append(url_without_fragment)
    except Exception as e:
        pass
    
    return links

def process_page_analytics(url, soup):
    # given the defragmented URL already
    analytics['pages_processed'] += 1
    analytics['unique_urls'].add(url)

    for script_or_style in soup(['script', 'style']):
        script_or_style.decompose()

    text = soup.get_text()

    # extract alphanumeric words
    tokens = re.findall(r'[a-zA-Z0-9]+', text.lower())

    # filter out stop words and 1-character words
    # SHOULD WE FILTER OUT 1 CHAR WORDS?
    words = [token for token in tokens if token not in STOP_WORDS and len(token) > 1]

    # word frequencies
    for word in words:
        analytics['word_frequencies'][word] = analytics['word_frequencies'].get(word, 0) + 1

    # longest page
    word_count = len(words)
    if word_count > analytics['longest_page']['word_count']:
        analytics['longest_page']['url'] = url
        analytics['longest_page']['word_count'] = word_count

    # subdomain and track pages per subdomain
    parsed = urlparse(url)
    subdomain = parsed.netloc.lower()
    if subdomain not in analytics['subdomains']:
        analytics['subdomains'][subdomain] = set()
    analytics['subdomains'][subdomain].add(url)

def is_valid(url):
    # Decide whether to crawl this url or not.
    # If you decide to crawl it, return True; otherwise return False.
    # There are already some conditions that return False.

    # Hard-coded limit for testing: stop after 50 pages
    if TESTING_LIMIT >= 0 and analytics['pages_processed'] >= TESTING_LIMIT:
        return False

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
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz"
            + r"|webp|avif|svg|webm|flv"
            + r"|woff|woff2|ttf|otf|eot"
            + r"|img|apk|war|sql|db|bak)$", parsed.path.lower()):
            return False
        
        # add our "is this url good" rules here
        if _is_calendar_path(parsed):
            return False

        if _has_file_extension_in_query(parsed):
            return False

        if _is_wiki_trap(parsed):
            return False

        return True

    except (TypeError, AttributeError) as e:
        return False

def generate_report(filename="report.txt"):
    with open(filename, 'w') as f:
        f.write(f"unique_pages: {len(analytics['unique_urls'])}\n")
        f.write(f"longest_page: {analytics['longest_page']}\n")
        f.write(f"top_50_words: {sorted(analytics['word_frequencies'].items(), key=lambda x: x[1], reverse=True)[:50]}\n")
        f.write("subdomains:\n")
        for subdomain in sorted(analytics['subdomains'].keys()):
            f.write(f"{subdomain} {len(analytics['subdomains'][subdomain])}\n")
    print(f"Report saved to {filename}")
