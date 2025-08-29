import io

import numpy as np
import soundfile as sf
from faster_whisper import WhisperModel
from lhotse import MonoCut, Recording
from lhotse.cut.mono import SupervisionSegment

from ccaudio.preprocess.whisper_detect_lang import whisper_detect_lang


def test_whisper_detect_lang() -> None:
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
        supervisions=[
            SupervisionSegment(
                id="segment_id",
                recording_id=recording.id,
                start=0,
                duration=recording.duration,
                channel=0,
            )
        ],
    )

    model = WhisperModel(model_size_or_path="large-v3")

    cut = whisper_detect_lang(cut, model)
    assert isinstance(cut, MonoCut)
