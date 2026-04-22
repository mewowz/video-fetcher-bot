from pathlib import Path
#from worker.downloader import Downloader
import worker.downloader as dler
import pytest


def local_downloader(name="local_downloader", dl_type="local", ytdlp_opts={}, downloader_opts={}):
    return dler.Downloader(name, dl_type=dl_type, ytdlp_opts=ytdlp_opts, downloader_opts=downloader_opts)

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
