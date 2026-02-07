import re
from urllib.parse import urlparse, parse_qs


EXACT_TRAP_PARAMS = {'q', 's'}
CONTAINS_TRAP_PARAMS = {'search', 'query', 'filter', 'share'}
MAX_PAGINATION_PAGE = 5
PAGINATION_QUERY_PARAMS = ['p=', 'cat=', 'author=', 'page_id=']


class UrlFilter:
    def __init__(self, allowed_domain_patterns, binary_extensions):
        self._allowed_domain_patterns = allowed_domain_patterns
        self._binary_extensions = binary_extensions

    def is_valid(self, url):
        try:
            parsed = urlparse(url)
            if parsed.scheme not in {"http", "https"}:
                return False
            if not parsed.netloc:
                return False
            domain = parsed.netloc.lower()
            if not any(p.match(domain) for p in self._allowed_domain_patterns):
                return False
            path_lower = parsed.path.lower()
            if any(path_lower.endswith(ext) for ext in self._binary_extensions):
                return False
            if self._is_login_page(parsed):
                return False
            if self._is_calendar_path(parsed):
                return False
            if self._has_file_extension_in_query(parsed):
                return False
            if self._is_wiki_trap(parsed):
                return False
            if self._is_gitlab_trap(parsed):
                return False
            if self._is_display_html_trap(parsed):
                return False
            if self._is_eppstein_filtered_path(parsed):
                return False
            if self._is_video_path(parsed):
                return False
            if self._is_slide_page(parsed):
                return False
            if self._is_numbered_report_page(parsed):
                return False
            if self._is_pagination_trap(parsed):
                return False
            if self._is_search_or_filter_page(parsed):
                return False
            if self._is_dir_listing_sort_query(parsed):
                return False
            return True
        except (TypeError, AttributeError):
            return False

    def _is_calendar_path(self, parsed_url):
        path = parsed_url.path.lower()
        segments = [s for s in path.split('/') if s]
        for i in range(len(segments) - 1):
            if segments[i].isdigit() and len(segments[i]) == 4:
                y = int(segments[i])
                if 1900 <= y <= 2099 and i + 1 < len(segments) and segments[i + 1].isdigit():
                    m = int(segments[i + 1])
                    if 1 <= m <= 12:
                        return True
        if re.search(r'\b\d{4}-\d{2}(-\d{2})?\b', path):
            return True
        if re.search(r'/events/(month|day|week|list|category)/', path):
            return True
        if '/timeline' in path:
            return True
        if parsed_url.query:
            q = parsed_url.query.lower()
            if any(p in q for p in ['date', 'year', 'month', 'day', 'time', 'timestamp', 'ical']):
                return True
        return False

    def _has_file_extension_in_query(self, parsed_url):
        if not parsed_url.query:
            return False
        q = parsed_url.query.lower()
        if any(a in q for a in ['do=media', 'action=download', 'action=export']):
            return True
        return any(ext in q for ext in self._binary_extensions)

    def _is_gitlab_trap(self, parsed_url):
        if 'gitlab' not in parsed_url.netloc.lower():
            return False
        path = parsed_url.path.lower()
        traps = ['/-/commit', '/-/blob', '/-/tree', '/-/raw', '/-/blame', '/-/compare',
                 '/-/merge_requests', '/-/jobs', '/-/pipelines', '/-/network', '/-/tags',
                 '/-/commits', '/-/branches', '/-/issues', '/-/milestones', '/-/releases', '/-/forks']
        return any(t in path for t in traps)

    def _is_login_page(self, parsed_url):
        return bool(re.search(r'(wp-login|login|signin|sign-in)\.php', parsed_url.path.lower()))

    def _is_search_or_filter_page(self, parsed_url):
        if not parsed_url.query:
            return False
        try:
            keys = [k.lower() for k in parse_qs(parsed_url.query, keep_blank_values=True)]
        except Exception:
            return False
        for k in keys:
            if k in EXACT_TRAP_PARAMS or any(c in k for c in CONTAINS_TRAP_PARAMS):
                return True
        return False

    def _is_dir_listing_sort_query(self, parsed_url):
        if not parsed_url.query:
            return False
        q = parsed_url.query.lower()
        return 'c=' in q and 'o=' in q

    def _is_display_html_trap(self, parsed_url):
        return bool(re.search(r'display\.html/.+', parsed_url.path.lower()))

    def _is_eppstein_filtered_path(self, parsed_url):
        segments = [s.lower() for s in parsed_url.path.split('/') if s]
        return bool(segments and {'junkyard', 'pix', 'ca', 'untetra'} & set(segments))

    def _is_video_path(self, parsed_url):
        segments = [s.lower() for s in parsed_url.path.split('/') if s]
        return 'videos' in segments or 'video' in segments

    def _is_slide_page(self, parsed_url):
        return bool(re.search(r'/(?:t)?sld\d+\.htm(l?)$', parsed_url.path.lower()))

    def _is_pagination_trap(self, parsed_url):
        path = parsed_url.path.lower()
        m = re.search(r'/page/(\d+)(?:/|$)', path)
        if m and int(m.group(1)) > MAX_PAGINATION_PAGE:
            return True
        if parsed_url.query and any(p in parsed_url.query.lower() for p in PAGINATION_QUERY_PARAMS):
            return True
        return False

    def _is_numbered_report_page(self, parsed_url):
        return bool(re.search(r'/r\d+a?\.html$', parsed_url.path.lower()))

    def _is_wiki_trap(self, parsed_url):
        if not parsed_url.query:
            return False
        q = parsed_url.query.lower()
        path = parsed_url.path.lower()
        if 'doku.php' in path and any(t in q for t in ['do=', 'idx=', 'sectok=']):
            return True
        if any(t in q for t in ['version=', 'action=diff', 'format=txt']):
            return True
        return False
