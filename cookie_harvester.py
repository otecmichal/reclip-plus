import os
import time
import threading
import subprocess

COOKIE_FILE = os.path.join(os.path.dirname(__file__), "cookies.txt")

def harvest_cookies():
    while True:
        try:
            # Lean cookie harvesting using curl to get a basic cookie jar from YouTube
            subprocess.run(["curl", "-c", COOKIE_FILE, "-s", "https://www.youtube.com"], check=False)
            print("Cookie jar updated via curl")
        except Exception as e:
            print(f"Error harvesting cookies: {e}")
        
        # Sleep for 24 hours
        time.sleep(86400)

def start_harvester():
    thread = threading.Thread(target=harvest_cookies, daemon=True)
    thread.start()
