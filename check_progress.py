import shelve
import sys
from configparser import ConfigParser

WIDTH = 50

def _rule():
    return "=" * (WIDTH + 15)

def _row(label, value):
    print("  {:<{}}  {}".format(str(label), WIDTH, value))

def check_progress(save_file="frontier.shelve"):
    try:
        save = shelve.open(save_file, writeback=False)
        total_urls = len(save)
        completed = 0
        pending = 0

        for url, is_completed in save.values():
            if is_completed:
                completed += 1
            else:
                pending += 1

        save.close()

        print("\n" + _rule())
        print("  Frontier Progress (" + str(save_file) + ")")
        print(_rule())
        _row("Total URLs discovered", total_urls)
        _row("Completed", completed)
        _row("Pending", pending)
        if total_urls > 0:
            pct = (completed / total_urls) * 100
            print("  {:<{}}  {:.2f}%".format("Progress", WIDTH, pct))

    except FileNotFoundError:
        print("Save file '" + str(save_file) + "' not found.")
        return
    except Exception as e:
        print("Error reading save file:", str(e))
        return

    _print_report()


def _print_report(analytics_file="analytics.shelve"):
    data = None
    try:
        from utils.analytics import load_analytics, analytics
        data = load_analytics(analytics_file)
        if data is None and analytics.get('unique_urls'):
            data = {
                'unique_urls': analytics.get('unique_urls', set()),
                'longest_page': analytics.get('longest_page', {'url': None, 'word_count': 0}),
                'word_frequencies': analytics.get('word_frequencies', {}),
                'subdomains': analytics.get('subdomains', {}),
                'pages_processed': analytics.get('pages_processed', 0),
                'skipped_not_200': analytics.get('skipped_not_200', 0),
                'skipped_empty_or_size': analytics.get('skipped_empty_or_size', 0),
                'skipped_duplicate': analytics.get('skipped_duplicate', 0),
                'skipped_error': analytics.get('skipped_error', 0),
                'skipped_url_filter': analytics.get('skipped_url_filter', 0),
            }
    except ImportError:
        pass
    if data is None:
        print("No analytics data (run crawler first; analytics saved on exit).")
        return

    unique_urls = data.get('unique_urls', set())
    longest = data.get('longest_page', {'url': None, 'word_count': 0})
    word_freqs = data.get('word_frequencies', {})
    subdomains = data.get('subdomains', {})

    # Q1: Unique pages
    print("\n" + _rule())
    print("  Q1: Unique Pages")
    print(_rule())
    _row("Count", len(unique_urls))

    # Q2: Longest page by word count
    print("\n" + _rule())
    print("  Q2: Longest Page (by word count)")
    print(_rule())
    print("  " + str(longest.get("url") or ""))
    _row("Words", longest.get("word_count", 0))

    # Q3: 50 most common words (excluding stop words)
    top_50 = sorted(word_freqs.items(), key=lambda x: x[1], reverse=True)[:50]
    print("\n" + _rule())
    print("  Q3: 50 Most Common Words")
    print(_rule())
    for rank, (word, count) in enumerate(top_50, 1):
        print("  {:>3}. {:<{}} {}".format(rank, str(word), WIDTH-4, count))

    # Tracked word sources
    word_sources = data.get('word_sources', {})
    for tracked_word in sorted(word_sources.keys()):
        sources = word_sources[tracked_word]
        top_sources = sorted(sources.items(), key=lambda x: x[1], reverse=True)[:20]
        print("\n" + _rule())
        print('  Tracked Word "{}" — Top {} Source URLs'.format(tracked_word, len(top_sources)))
        print(_rule())
        for rank, (src_url, cnt) in enumerate(top_sources, 1):
            print("  {:>3}. {} — {}".format(rank, src_url, cnt))

    # Q4: Subdomains in *.ics.uci.edu (alphabetical, with page counts)
    ics_subdomains = {
        sub: urls for sub, urls in subdomains.items()
        if sub.endswith(".uci.edu")
    }
    print("\n" + _rule())
    print("  Q4: Subdomains ({} found)".format(len(ics_subdomains)))
    print(_rule())
    for sub in sorted(ics_subdomains.keys()):
        _row(str(sub), len(ics_subdomains[sub]))

    # Skip stats (page-level = per fetch; URL filter = per outlink rejected)
    page_skip_keys = [
        ("skipped_not_200", "Non-200 status"),
        ("skipped_empty_or_size", "Empty / bad size"),
        ("skipped_duplicate", "Duplicate content"),
        ("skipped_error", "Error (parse/exception)"),
    ]
    total_page_skipped = sum(data.get(k, 0) for k, _ in page_skip_keys)
    url_filter_count = data.get("skipped_url_filter", 0)
    print("\n" + _rule())
    print("  Skip Stats")
    print(_rule())
    _row("Pages processed", data.get("pages_processed", 0))
    print("-" * (WIDTH + 15))
    for key, label in page_skip_keys:
        _row(label, data.get(key, 0))
    print("-" * (WIDTH + 15))
    _row("Total pages skipped", total_page_skipped)
    _row("Outlinks rejected by URL filter", url_filter_count)
    print(_rule() + "\n")


if __name__ == "__main__":
    save_file = "frontier.shelve"

    if len(sys.argv) > 1:
        if sys.argv[1] == "--config":
            config_file = sys.argv[2] if len(sys.argv) > 2 else "config.ini"
            cparser = ConfigParser()
            cparser.read(config_file)
            save_file = cparser["LOCAL PROPERTIES"]["SAVE"]
        else:
            save_file = sys.argv[1]

    check_progress(save_file)
