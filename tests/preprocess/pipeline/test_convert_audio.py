import io

import numpy as np
import soundfile as sf
from lhotse import MonoCut, MultiCut, Recording

from ccaudio.preprocess.pipeline.convert_audio import convert_audio


def test_convert_audio_multi() -> None:
    wav = np.random.randn(2, 16000)
    sr = 16000

    buf = io.BytesIO()
    sf.write(buf, wav.T, sr, format="WAV")

    recording = Recording.from_bytes(buf.getvalue(), recording_id="recording_id")
    assert recording.channel_ids is not None

    cut = MultiCut(
        id="id",
        start=0,
        duration=recording.duration,
        channel=recording.channel_ids,
        recording=recording,
    )
    cut = convert_audio(cut)

    assert isinstance(cut, MonoCut)


def test_convert_audio_mono() -> None:
    wav = np.random.randn(16000)
    sr = 16000

    buf = io.BytesIO()
    sf.write(buf, wav.T, sr, format="WAV")

    recording = Recording.from_bytes(buf.getvalue(), recording_id="recording_id")
    assert recording.channel_ids is not None

    cut = MonoCut(
        id="id",
        start=0,
        duration=recording.duration,
        channel=0,
        recording=recording,
    )
    cut = convert_audio(cut)

    assert isinstance(cut, MonoCut)
