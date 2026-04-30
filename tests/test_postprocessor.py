import pytest
import orjson as json
from pathlib import Path
from worker.postprocessor import PostProcessor, PostProcessResult

@pytest.mark.asyncio
@pytest.mark.unit
async def test_get_ffprobe_json(monkeypatch):
    postprocessor = PostProcessor("name")

    expected = {"testing": "123"}
    async def mock_ffprobe_runner(_):
        return (0, json.dumps(expected), bytes())
    monkeypatch.setattr(postprocessor, "_run_ffprobe", mock_ffprobe_runner)

    got = await postprocessor._get_ffprobe_json("somepathstr")
    assert got == expected


    async def mock_ffprobe_runner(_):
        return (1, bytes(), bytes("blahblah", "utf-8"))
    monkeypatch.setattr(postprocessor, "_run_ffprobe", mock_ffprobe_runner)
    with pytest.raises(RuntimeError):
        got = await postprocessor._get_ffprobe_json("blah")


@pytest.mark.asyncio
@pytest.mark.unit
async def test_is_mp4():
    postprocessor = PostProcessor("name")

    expected = True
    test_input = {"format": {"format_name": "something,mp4,sometingelse"}}
    got = postprocessor._is_mp4(test_input)

    assert got == expected


    expected = False
    test_input = {"format": {"format_name": "something,sometingelse"}}
    got = postprocessor._is_mp4(test_input)

    assert got == expected


@pytest.mark.asyncio
@pytest.mark.unit
async def test_is_mpegts():
    postprocessor = PostProcessor("name")

    expected = True
    test_input = {"format": {"format_name": "something,mpegts,sometingelse"}}
    got = postprocessor._is_mpegts(test_input)

    assert got == expected


    expected = False
    test_input = {"format": {"format_name": "something,sometingelse"}}
    got = postprocessor._is_mpegts(test_input)

    assert got == expected


async def test_is_webm():
    # TODO: implement this test after writing the actual PostProcessor._is_webm
    pass


@pytest.mark.asyncio
@pytest.mark.unit
async def test_convert_mpegts_to_mp4(monkeypatch):
    postprocessor = PostProcessor("name")
    video_path = Path("some/path/to/video.mp4")

    async def mock_try_copy(x, y):
        return (False, 1)
    monkeypatch.setattr(postprocessor, "_mpegtsmp4_try_copy", mock_try_copy)
   
    expected_path = Path("some/path/to/video_c.mp4")
    expected_ec = 0
    async def mock_try_reencode(x, y):
        return (True, expected_ec)
    
    monkeypatch.setattr(postprocessor, "_mpegtsmp4_try_reencode_h264", mock_try_reencode)

    got_path, got_ec = await postprocessor._convert_mpegts_to_mp4(video_path)
    assert got_path == expected_path
    assert got_ec == expected_ec

    expected_path = None
    expected_ec = 1
    async def mock_try_reencode(x, y):
        return (False, expected_ec)
    monkeypatch.setattr(postprocessor, "_mpegtsmp4_try_reencode_h264", mock_try_reencode)

    got_path, got_ec = await postprocessor._convert_mpegts_to_mp4(video_path)
    assert got_path == expected_path
    assert got_ec == expected_ec


    expected_path = Path("some/path/to/video_c.mp4")
    expected_ec = 0
    async def mock_try_copy(x, y):
        return (True, 0)
    monkeypatch.setattr(postprocessor, "_mpegtsmp4_try_copy", mock_try_copy)
    got_path, got_ec = await postprocessor._convert_mpegts_to_mp4(video_path)
    assert got_path == expected_path
    assert got_ec == expected_ec
