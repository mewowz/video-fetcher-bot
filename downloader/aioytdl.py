import asyncio
from yt_dlp import YoutubeDL
from dataclasses import dataclass
from pathlib import Path
from discord

VIDEO_OPTS = {
        "outtmpl": "videos/%(title)s.%(ext)s",
        "quality": "bv*+ba/best",
        "noplaylist": True
}

@dataclass
def DownloadJob:
    url: str


def download_video(
