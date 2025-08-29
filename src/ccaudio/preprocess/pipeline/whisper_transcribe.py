from dataclasses import asdict, is_dataclass

import numpy as np
from faster_whisper import WhisperModel
from lhotse import MonoCut
from lhotse.supervision import AlignmentItem


def whisper_transcribe(cut: MonoCut, model: WhisperModel) -> MonoCut:
    audio = cut.load_audio()
    assert isinstance(audio, np.ndarray)

    segments, _ = model.transcribe(
        audio=audio[0], language=cut.supervisions[0].language
    )

    pred_text = ""
    alignment_items = []
    for segment in segments:
        seg = serialize(segment)

        pred_text += str(seg["text"]).strip()  # type: ignore
        alignment_items.append(
            AlignmentItem(
                symbol=str(seg["text"]),  # type: ignore
                start=float(seg["start"]),  # type: ignore
                duration=float(seg["end"]) - float(seg["start"]),  # type: ignore
            )
        )

    s = cut.supervisions[0]
    s.text = pred_text
    s.alignment = {"word": alignment_items}
    cut.supervisions = [s]

    return cut


def serialize(obj):
    if is_dataclass(obj) and not isinstance(obj, type):
        return asdict(obj)
    elif isinstance(obj, list):
        return [serialize(item) for item in obj]
    elif isinstance(obj, dict):
        return {k: serialize(v) for k, v in obj.items()}
    return obj
