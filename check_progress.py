import shelve
import sys
from configparser import ConfigParser


def check_progress(save_file="frontier.shelve"):
    try:
        save = shelve.open(save_file)
        total_urls = len(save)
        completed = 0
        pending = 0

        for url, is_completed in save.values():
            if is_completed:
                completed += 1
            else:
                pending += 1

        save.close()

        print(f"\n{'='*60}")
        print(f"  Frontier Progress ({save_file})")
        print(f"{'='*60}")
        print(f"  Total URLs discovered : {total_urls}")
        print(f"  Completed             : {completed}")
        print(f"  Pending               : {pending}")
        if total_urls > 0:
            print(f"  Progress              : {(completed / total_urls) * 100:.2f}%")

    except FileNotFoundError:
        print(f"Save file '{save_file}' not found.")
        return
    except Exception as e:
        print(f"Error reading save file: {e}")
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
    print(f"\n{'='*60}")
    print("  Q1: Unique Pages")
    print(f"{'='*60}")
    print(f"  Count: {len(unique_urls)}")

    # Q2: Longest page by word count
    print(f"\n{'='*60}")
    print("  Q2: Longest Page (by word count)")
    print(f"{'='*60}")
    print(f"  URL  : {longest['url']}")
    print(f"  Words: {longest['word_count']}")

    # Q3: 50 most common words (excluding stop words)
    top_50 = sorted(word_freqs.items(), key=lambda x: x[1], reverse=True)[:50]
    print(f"\n{'='*60}")
    print("  Q3: 50 Most Common Words")
    print(f"{'='*60}")
    for rank, (word, count) in enumerate(top_50, 1):
        print(f"  {rank:>3}. {word:<25} {count}")

    # Q4: Subdomains in *.ics.uci.edu (alphabetical, with page counts)
    ics_subdomains = {
        sub: urls for sub, urls in subdomains.items()
        if sub.endswith('.uci.edu')
    }
    print(f"\n{'='*60}")
    print(f"  Q4: Subdomains ({len(ics_subdomains)} found)")
    print(f"{'='*60}")
    for sub in sorted(ics_subdomains.keys()):
        print(f"  {sub}, {len(ics_subdomains[sub])}")

    # Skip stats (page-level = per fetch; URL filter = per outlink rejected)
    page_skip_keys = [
        ('skipped_not_200',       'Non-200 status'),
        ('skipped_empty_or_size', 'Empty / bad size'),
        ('skipped_duplicate',     'Duplicate content'),
    ]
    total_page_skipped = sum(data.get(k, 0) for k, _ in page_skip_keys)
    url_filter_count = data.get('skipped_url_filter', 0)
    print(f"\n{'='*60}")
    print("  Skip Stats")
    print(f"{'='*60}")
    print(f"  Pages processed   : {data.get('pages_processed', 0)}")
    for key, label in page_skip_keys:
        print(f"  {label:<22}: {data.get(key, 0)}")
    print(f"  {'Total pages skipped':<22}: {total_page_skipped}")
    print(f"  {'URL filter (outlinks)':<22}: {url_filter_count}")
    print(f"{'='*60}\n")


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
