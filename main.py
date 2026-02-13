import redis
import json
import sqlite3
import os
from yt_dlp import YoutubeDL

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = os.environ.get("REDIS_PORT", 6379)
REDIS_DB = os.environ.get("REDIS_DB", 0)
MUSIC_DIR = os.environ.get("MUSIC_DIR")
DB_FILE = os.environ.get("DB_FILE", "musics.db")

if not MUSIC_DIR:
    raise Exception("MUSIC_DIR is not set")

os.makedirs(MUSIC_DIR, exist_ok=True)

SSE = "ytjob:sse"
QUEUE = "ytjob:queue"
PROCESSING = "ytjob:processing"

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
db = sqlite3.connect(DB_FILE)

db.execute("CREATE TABLE IF NOT EXISTS music (id INTEGER PRIMARY KEY AUTOINCREMENT, author STRING NOT NULL, name STRING NOT NULL, path STRING NOT NULL, report TEXT, time INTEGER DEFAULT 0, unique(path));")

insert_music_qry = "INSERT INTO music (author, name, path, time, report) VALUES (:author, :name, :path, :time, :report) ON CONFLICT(path) DO UPDATE SET report = :report, time = :time"


def main():
    if not r.ping():
        raise Exception("Failed to connect to Redis")

    while True:
        job_str = r.brpoplpush(QUEUE, PROCESSING, timeout=0)
        job = Job.from_str(job_str)
        if not job:
            # Invalid job, remove from processing queue
            r.lrem(PROCESSING, 1, job_str)
            continue

        job.download()
        r.lrem(PROCESSING, 1, job_str)

class Job:
    @staticmethod
    def from_str(job_str):
        try:
            json_job = json.loads(job_str)
            if not json_job['author'] or not json_job['title'] or not json_job['url']:
                return None
            return Job(json_job['url'], json_job['author'], json_job['title'])
        except Exception as e:
            return None
    def __init__(self, url, author, title):
        self.url = url
        self.author = author
        self.title = title
        self.duration = None
    
    def __create_artist_folder(self):
        artist_folder = os.path.join(MUSIC_DIR, self.author)
        os.makedirs(artist_folder, exist_ok=True)
        return artist_folder
    
    def __final_path(self):
        return os.path.join(MUSIC_DIR, self.author, self.title + ".%(ext)s")
    
    def __rel_path(self):
        return os.path.join("/Musica/", self.author, self.title + ".mp3")

    def __download_mp3(self):
        ytdl_opts = {
            "outtmpl": self.__final_path(),
            "format": "bestaudio/best",
            "noplaylist": True,
            "quiet": True,
            "postprocessors": [{
                "key": "FFmpegExtractAudio",
                "preferredcodec": "mp3",
                "preferredquality": 192,
            }],
            "postprocessor_args": [
                "-metadata", f"title={self.title}",
                "-metadata", f"artist={self.author}",
            ],
        }
        with YoutubeDL(ytdl_opts) as ydl:
            info = ydl.extract_info(self.url, download=False)
            ydl.download([self.url])
            self.duration = info.get('duration', None)
    
    def download(self):
        self.__create_artist_folder()
        self.__download_mp3()

        db.execute(insert_music_qry, {"author": self.author, "name": self.title, "path": self.__rel_path(), "time": self.duration, "report": None})
        db.commit()






if __name__ == "__main__":
    main()