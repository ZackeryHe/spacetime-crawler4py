import shelve
import threading

from utils import get_logger

_logger = get_logger("ANALYTICS")

analytics = {
    'unique_urls': set(),
    'longest_page': {'url': None, 'word_count': 0},
    'word_frequencies': {},
    'subdomains': {},
    'pages_processed': 0,
    'report_generated': False,
    'skipped_not_200': 0,
    'skipped_empty_or_size': 0,
    'skipped_duplicate': 0,
    'skipped_url_filter': 0,
}

ANALYTICS_SAVE_FILE = "analytics.shelve"
_analytics_save_interval = 50
_last_analytics_save_at = 0
_analytics_save_lock = threading.Lock()


def maybe_save_analytics():
    global _last_analytics_save_at
    with _analytics_save_lock:
        if analytics['pages_processed'] - _last_analytics_save_at >= _analytics_save_interval:
            _last_analytics_save_at = analytics['pages_processed']
            save_analytics()


def save_analytics(path=None):
    path = path or ANALYTICS_SAVE_FILE
    try:
        data = {
            'pages_processed': analytics['pages_processed'],
            'skipped_not_200': analytics['skipped_not_200'],
            'skipped_empty_or_size': analytics['skipped_empty_or_size'],
            'skipped_duplicate': analytics['skipped_duplicate'],
            'skipped_url_filter': analytics['skipped_url_filter'],
            'unique_urls': list(analytics['unique_urls']),
            'longest_page': dict(analytics['longest_page']),
            'word_frequencies': dict(analytics['word_frequencies']),
            'subdomains': {k: list(v) for k, v in analytics['subdomains'].items()},
        }
        with shelve.open(path, 'c') as sh:
            for k, v in data.items():
                sh[k] = v
            if hasattr(sh, 'sync'):
                sh.sync()
    except Exception as e:
        _logger.error(f"Failed to save analytics: {e}")


def load_analytics(path=None):
    path = path or ANALYTICS_SAVE_FILE
    try:
        with shelve.open(path, 'r') as sh:
            return {
                'unique_urls': set(sh.get('unique_urls', [])),
                'longest_page': dict(sh.get('longest_page', {'url': None, 'word_count': 0})),
                'word_frequencies': dict(sh.get('word_frequencies', {})),
                'subdomains': {k: set(v) for k, v in sh.get('subdomains', {}).items()},
                'pages_processed': sh.get('pages_processed', 0),
                'skipped_not_200': sh.get('skipped_not_200', 0),
                'skipped_empty_or_size': sh.get('skipped_empty_or_size', 0),
                'skipped_duplicate': sh.get('skipped_duplicate', 0),
                'skipped_url_filter': sh.get('skipped_url_filter', 0),
            }
    except Exception:
        return None


def restore_analytics(path=None):
    global _last_analytics_save_at
    data = load_analytics(path)
    if data is None:
        return
    analytics['unique_urls'] = data['unique_urls']
    analytics['longest_page'] = data['longest_page']
    analytics['word_frequencies'] = data['word_frequencies']
    analytics['subdomains'] = data['subdomains']
    analytics['pages_processed'] = data['pages_processed']
    analytics['skipped_not_200'] = data['skipped_not_200']
    analytics['skipped_empty_or_size'] = data['skipped_empty_or_size']
    analytics['skipped_duplicate'] = data['skipped_duplicate']
    analytics['skipped_url_filter'] = data['skipped_url_filter']
    _last_analytics_save_at = analytics['pages_processed']
