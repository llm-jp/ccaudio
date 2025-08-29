import io

import numpy as np
import soundfile as sf
from faster_whisper import WhisperModel
from lhotse import MonoCut, Recording, SupervisionSegment

from ccaudio.preprocess.pipeline.whisper_transcribe import whisper_transcribe


def test_whisper_transcribe() -> None:
    wav = np.random.randn(16000 * 30)
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

    whisper_transcribe(cut, model)
