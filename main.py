import redis
import json
import sqlite3
import os
from yt_dlp import YoutubeDL
from dotenv import load_dotenv
from psycopg import connect
from datetime import datetime
import tempfile

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
TMP_FOLDER = os.environ.get("TMP_FOLDER")

if not MUSIC_FOLDER:
    raise Exception("MUSIC_FOLDER is not set")

if not TMP_FOLDER:
    TMP_FOLDER = tempfile.mkdtemp()
    print(f"TMP_FOLDER: {TMP_FOLDER}")

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

class Job:
    @staticmethod
    def from_str(job_str):
        json_job = json.loads(job_str)
        if not json_job['url']:
            raise Exception("Invalid job", "url is required")
        if json_job.get('temporary', False):
            return TemporaryJob(json_job['url'])
        if json_job.get('artist', None) and json_job.get('title', None):
            return PersistentJob(json_job['url'], json_job['artist'], json_job['title'])
        
        raise Exception("Not a valid job, missing url or artist and title")
        
    
    def __init__(self, url):
        self.url = url
        self.filename = None
        self.duration = None

    def __download_mp3(self):
         with YoutubeDL(self.ytdl_opts()) as ydl:
            info = ydl.extract_info(self.url, download=False)
            self.duration = info.get('duration', None)
            try:
                self.filename = ydl.prepare_filename(info)
                ydl.download([self.url])
            except Exception as e:
                self.error(str(e))
    
    def error(self, message):
        notify({
            "error": message,
            "url": self.url,
        })

    def download(self):
        self.prepare()
        self.__download_mp3()
        self.post()

    def final_path(self):
        raise NotImplementedError("final_path must be implemented by the subclass")
    
    def ytdl_opts(self):
        return {
            "outtmpl": self.final_path(),
            "format": "bestaudio/best",
            "noplaylist": True,
            "quiet": True,
            "progress_hooks": [self.progress],
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
            "postprocessor_args": [],
        }
    
    def prepare(self):
        pass
    
    def progress(self, d):
        pass

    def post(self):
        pass


class TemporaryJob(Job):
    def __init__(self, url):
        Job.__init__(self, url)
        self.temporary = True
    
    def final_path(self):
        return os.path.join(TMP_FOLDER, "%(id)s.%(ext)s")
    
    def post(self):
        # get base name without extension
        name = os.path.splitext(os.path.basename(self.filename))[0]
        notify({
            "name": name,
            "done": True,
        })

class PersistentJob(Job):
    def __init__(self, url, artist, title):
        Job.__init__(self, url)
        self.artist = artist
        self.title = title
    
    def __create_artist_folder(self):
        artist_folder = os.path.join(MUSIC_FOLDER, self.artist)
        os.makedirs(artist_folder, exist_ok=True)
        return artist_folder
    
    def __insert_into_database(self):
        cur = conn.cursor()
        cur.execute("INSERT INTO music (artist, title, duration, path, notes) VALUES (%s, %s, %s, %s, '') ON CONFLICT (path) DO UPDATE SET duration = %s, artist = %s, title = %s", (self.artist, self.title, self.duration, self.__rel_path(), self.duration, self.artist, self.title))
        cur.close()
        conn.commit()
    
    def final_path(self):
        return os.path.join(MUSIC_FOLDER, self.artist, self.title + ".%(ext)s")
    
    def __rel_path(self):
        return os.path.join(self.artist, self.title + ".mp3")

    def ytdl_opts(self):
        opts = Job.ytdl_opts(self)
        opts["postprocessor_args"].extend([
            "-metadata", f"title={self.title}",
            "-metadata", f"artist={self.artist}",
        ])
        return opts
    
    def prepare(self):
        self.__create_artist_folder()
    
    def progress(self, d):
        return notify({
            "artist": self.artist,
            "title": self.title,
            "progress": {
                "downloaded_bytes": d.get('downloaded_bytes', 0),
                "total_bytes": d.get('total_bytes', 0),
                "elapsed_seconds": d.get('elapsed', 0),
            },
        })

    def post(self):
        self.__insert_into_database()
        notify({
            "artist": self.artist,
            "title": self.title,
            "duration": self.duration,
            "path": self.__rel_path(),
            "notes": "",
        })



def main():
    if not r.ping():
        raise Exception("Failed to connect to Redis")
    
    notify("Worker started")
    while True:
        job_str = r.brpoplpush(QUEUE, PROCESSING, timeout=0)
        try:
            job = Job.from_str(job_str)
            job.download()
        except Exception as e:
            notify({"error": str(e)})
            r.lrem(PROCESSING, 1, job_str)
            continue
        
        r.lrem(PROCESSING, 1, job_str)

if __name__ == "__main__":
    main()