import redis
import sys
import json
import os
from yt_dlp import YoutubeDL

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = os.environ.get("REDIS_PORT", 6379)
REDIS_DB = os.environ.get("REDIS_DB", 0)

QUEUE = "ytjob:queue"

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)

url = sys.argv[1] if len(sys.argv) > 1 else None
if not url:
    print("Usage: python cli.py <url>")
    sys.exit(1)

def main():
    with YoutubeDL() as ydl:
        info = ydl.extract_info(url, download=False)
        video_title = info.get('title')
        default_author, default_title = video_title.split(" - ") if " - " in video_title else (video_title, "")
        author = input(f"Author [{default_author}]: ") or default_author
        title = input(f"Title [{default_title}]: ") or default_title
        res = r.lpush(QUEUE, json.dumps({"url": url, "author": author, "title": title}))
        print(f"Added to queue: {res}")

if __name__ == "__main__":
    main()