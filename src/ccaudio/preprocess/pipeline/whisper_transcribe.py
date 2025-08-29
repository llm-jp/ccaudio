from dataclasses import asdict, is_dataclass

import numpy as np
from faster_whisper import WhisperModel
from lhotse import MonoCut


def whisper_transcribe(cut: MonoCut, model: WhisperModel) -> MonoCut:
    audio = cut.load_audio()
    assert isinstance(audio, np.ndarray)

    segments, _ = model.transcribe(
        audio=audio[0], language=cut.supervisions[0].language
    )

    _segments = []
    for segment in segments:
        _segments.append(serialize(segment))
    segments = _segments

    pred_text = "".join(str(segment["text"]) for segment in segments).strip()

    for s in cut.supervisions:
        s.text = pred_text

    return cut


def serialize(obj):
    if is_dataclass(obj) and not isinstance(obj, type):
        return asdict(obj)
    elif isinstance(obj, list):
        return [serialize(item) for item in obj]
    elif isinstance(obj, dict):
        return {k: serialize(v) for k, v in obj.items()}
    return obj
