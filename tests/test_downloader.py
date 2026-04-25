from pathlib import Path
#from worker.downloader import Downloader
import worker.downloader as dler
from worker.downloader import Downloader
import pytest
from urllib.parse import quote


def local_downloader(name="local_downloader", dl_type="local", ytdlp_opts={}, downloader_opts={}):
    return Downloader(name, dl_type=dl_type, ytdlp_opts=ytdlp_opts, downloader_opts=downloader_opts)

def test_downloader_get_unique_dl_path(monkeypatch):
    downloader = local_downloader()

    uuid4_out = "123456789"
    monkeypatch.setattr(dler, "uuid4", lambda: uuid4_out)

    video_id = "ABC12"
    expected_ret = ( 
            Path("data") / Path("videos") / 
            Path(uuid4_out) / Path(video_id)
    )

    got = downloader._get_unique_dl_path(video_id, "local")

    assert got == expected_ret

    with pytest.raises(NotImplementedError) as got_exception:
        got = downloader._get_unique_dl_path(video_id, "remote")

    with pytest.raises(NotImplementedError) as got_exception:
        got = downloader._get_unique_dl_path(video_id, "tmp")



def test_downloader_make_dl_path(tmp_path):
    downloader = local_downloader()

    new_path = tmp_path / "new_path"
    downloader._make_dl_path(new_path)
    assert new_path.exists() == True and new_path.is_dir() == True

    exists_path = tmp_path / "exists_path"
    exists_path.mkdir()
    with pytest.raises(FileExistsError) as got_exception:
        got = downloader._make_dl_path(exists_path)

def test_downloader_estimate_mp4_size():
    downloader = local_downloader()
    fake_info_dict = {
        "url": quote("https://blahblah.com/sgoap/clen=100;dur=30.0;gir=yes;itag=140;lmt=12345/sgovp/clen=100;dur=30.0;gir=yes;itag=140;lmt=123456/somepadding/stuff/over/here"),
    }

    expected = 100+100
    got = downloader._estimate_mp4_size(fake_info_dict)
    assert got == expected

    fake_info_dict = {
        "url": quote("https://blahblah.com/sgoap/clen=100;dur=30.0;gir=yes;itag=140;lmt=12345/somepadding/stuff/over/here"),
    }

    expected = 100
    got = downloader._estimate_mp4_size(fake_info_dict)
    assert got == expected

    fake_info_dict = {
        "url": quote("https://blahblah.com/sgovp/clen=100;dur=30.0;gir=yes;itag=140;lmt=12345/somepadding/stuff/over/here"),
    }

    expected = 100
    got = downloader._estimate_mp4_size(fake_info_dict)
    assert got == expected


    fake_info_dict = {
        "url": quote("https://blahblah.com/somepadding/stuff/over/here"),
    }

    expected = 0
    got = downloader._estimate_mp4_size(fake_info_dict)
    assert got == expected

def test_video_size_ok(monkeypatch):
    downloader = local_downloader()

    fake_video_info = {
        "format_id": 1
    }

    fake_format_info = {
        "format_id": 1,
        "filesize": 1234
    }
    fake_video_info["formats"] = [fake_format_info]
    expected = (True, None)
    got = downloader._video_size_ok(fake_video_info)
    assert got == expected

    fake_format_info = {
        "format_id": 1,
        "filesize_approx": 1234
    }
    fake_video_info["formats"] = [fake_format_info]
    expected = (True, None)
    got = downloader._video_size_ok(fake_video_info)
    assert got == expected

    fake_format_info = {
        "format_id": 1,
        "filesize": 123400**2
    }
    fake_video_info["formats"] = [fake_format_info]
    expected = (False, Downloader.DOWNLOAD_ERROR.FILESIZE_TOO_BIG)
    got = downloader._video_size_ok(fake_video_info)
    assert got == expected

    monkeypatch.setattr(downloader, "_estimate_mp4_size", lambda _: 1234)
    fake_format_info = {
        "format_id": 1,
        "ext": "mp4",
    }
    fake_video_info["formats"] = [fake_format_info]
    expected = (True, None)
    got = downloader._video_size_ok(fake_video_info)
    assert got == expected


    monkeypatch.setattr(downloader, "_estimate_mp4_size", lambda _: 0)
    fake_format_info = {
        "format_id": 1,
        "ext": "mp4",
    }
    fake_video_info["formats"] = [fake_format_info]
    expected = (False, Downloader.DOWNLOAD_ERROR.CANNOT_DET_FILESIZE)
    got = downloader._video_size_ok(fake_video_info)
    assert got == expected

    monkeypatch.setattr(downloader, "_estimate_mp4_size", lambda _: 12456**2)
    fake_format_info = {
        "format_id": 1,
        "ext": "mp4",
    }
    fake_video_info["formats"] = [fake_format_info]
    expected = (False, Downloader.DOWNLOAD_ERROR.FILESIZE_TOO_BIG)
    got = downloader._video_size_ok(fake_video_info)
    assert got == expected

    fake_format_info = {
        "format_id": 1,
        "ext": "foo"
    }
    fake_video_info["formats"] = [fake_format_info]
    expected = (False, Downloader.DOWNLOAD_ERROR.CANNOT_DET_FILESIZE)
    got = downloader._video_size_ok(fake_video_info)
    assert got == expected
    
