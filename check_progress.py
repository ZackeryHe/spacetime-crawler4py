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
        
        print(f"\n{'='*50}")
        print(f"Progress Report for {save_file}")
        print(f"{'='*50}")
        print(f"Total URLs discovered: {total_urls}")
        print(f"Completed: {completed}")
        print(f"Pending: {pending}")
        if total_urls > 0:
            percentage = (completed / total_urls) * 100
            print(f"Progress: {percentage:.2f}%")
        print(f"{'='*50}\n")
        
    except FileNotFoundError:
        print(f"Save file '{save_file}' not found. Crawler hasn't started yet or was run with --restart.")
    except Exception as e:
        print(f"Error reading save file: {e}")

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
