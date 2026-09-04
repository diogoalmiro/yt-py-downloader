import redis
import json
import sqlite3
import os
from yt_dlp import YoutubeDL
from dotenv import load_dotenv
from psycopg import connect
from datetime import datetime

load_dotenv()

REDIS_HOST = os.environ.get("REDIS_HOST", "127.0.0.1")
REDIS_PORT = os.environ.get("REDIS_PORT", 6379)
REDIS_DB = os.environ.get("REDIS_DB", 0)

PG_HOST = os.environ.get("PG_HOST", "127.0.0.1")
PG_PORT = os.environ.get("PG_PORT", 5432)
PG_DATABASE = os.environ.get("PG_DATABASE", "music.diogoalmiro.com")
PG_USER = os.environ.get("PG_USER", "pi5")
PG_PASSWORD = os.environ.get("PG_PASSWORD")

MUSIC_FOLDER = os.environ.get("MUSIC_FOLDER")

if not MUSIC_FOLDER:
    raise Exception("MUSIC_FOLDER is not set")

os.makedirs(MUSIC_FOLDER, exist_ok=True)

NOTIFICATIONS = "ytjob:notifications"
QUEUE = "ytjob:queue"
PROCESSING = "ytjob:processing"

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)

def notify(message):
    date = datetime.now().isoformat()
    r.publish(NOTIFICATIONS, json.dumps({"date": date, "message": message}))
    print(date, json.dumps(message))

conn = connect(
    host=PG_HOST,
    port=PG_PORT,
    dbname=PG_DATABASE,
    user=PG_USER,
    password=PG_PASSWORD
)

def main():
    if not r.ping():
        raise Exception("Failed to connect to Redis")
    
    notify("Worker started")
    while True:
        job_str = r.brpoplpush(QUEUE, PROCESSING, timeout=0)
        job = Job.from_str(job_str)
        if not job:
            notify({
                "job_str": job_str,
                "error": "Invalid job, removing from processing queue",
            })
            # Invalid job, remove from processing queue
            r.lrem(PROCESSING, 1, job_str)
            continue
        job.download()
        r.lrem(PROCESSING, 1, job_str)

def progress_hook(artist, title):
    def progress(d):
        notify({
            "artist": artist,
            "title": title,
            "progress": {
                "downloaded_bytes": d.get('downloaded_bytes', 0),
                "total_bytes": d.get('total_bytes', 0),
                "elapsed_seconds": d.get('elapsed', 0),
            },
        })
    return progress

class Job:
    @staticmethod
    def from_str(job_str):
        try:
            json_job = json.loads(job_str)
            if not json_job['artist'] or not json_job['title'] or not json_job['url']:
                return None
            return Job(json_job['url'], json_job['artist'], json_job['title'])
        except Exception as e:
            return None
    def __init__(self, url, artist, title):
        self.url = url
        self.artist = artist
        self.title = title
        self.duration = None
    
    def __create_artist_folder(self):
        artist_folder = os.path.join(MUSIC_FOLDER, self.artist)
        os.makedirs(artist_folder, exist_ok=True)
        return artist_folder
    
    def __final_path(self):
        return os.path.join(MUSIC_FOLDER, self.artist, self.title + ".%(ext)s")
    
    def __rel_path(self):
        return os.path.join(self.artist, self.title + ".mp3")

    def __download_mp3(self):
        ytdl_opts = {
            "outtmpl": self.__final_path(),
            "format": "bestaudio/best",
            "noplaylist": True,
            "quiet": True,
            "progress_hooks": [progress_hook(artist=self.artist, title=self.title)],
            "extractor_args": {
                "youtube": {
                    "player_client": ["android", "ios"]
                }
            },
            "postprocessors": [{
                "key": "FFmpegExtractAudio",
                "preferredcodec": "mp3",
                "preferredquality": 192,
            }],
            "postprocessor_args": [
                "-metadata", f"title={self.title}",
                "-metadata", f"artist={self.artist}",
            ],
        }
        with YoutubeDL(ytdl_opts) as ydl:
            info = ydl.extract_info(self.url, download=False)
            self.duration = info.get('duration', None)
            try:
                ydl.download([self.url])
            except Exception as e:
                notify({
                    "error": str(e),
                    "artist": self.artist,
                    "title": self.title,
                })
    
    def __insert_into_database(self):
        cur = conn.cursor()
        cur.execute("INSERT INTO music (artist, title, duration, path, notes) VALUES (%s, %s, %s, %s, '') ON CONFLICT (path) DO UPDATE SET duration = %s, artist = %s, title = %s", (self.artist, self.title, self.duration, self.__rel_path(), self.duration, self.artist, self.title))
        cur.close()
        conn.commit()
    
    def download(self):
        self.__create_artist_folder()
        self.__download_mp3()
        self.__insert_into_database()
        notify({
            "artist": self.artist,
            "title": self.title,
            "duration": self.duration,
            "path": self.__rel_path(),
            "notes": "",
        })





if __name__ == "__main__":
    main()