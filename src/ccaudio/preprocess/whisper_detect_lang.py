import numpy as np
from faster_whisper import WhisperModel
from lhotse import MonoCut, SupervisionSegment


def whisper_detect_lang(cut: MonoCut, model: WhisperModel) -> MonoCut:
    audio = cut.load_audio()
    assert isinstance(audio, np.ndarray)

    features = model.feature_extractor(audio, chunk_length=30)

    lang, lang_prob, _ = model.detect_language(
        features=features,
        vad_filter=True,
        language_detection_segments=7,
        language_detection_threshold=0.5,
    )

    cut.supervisions = [
        SupervisionSegment(
            id=f"segment_{cut.id}",
            recording_id=cut.recording_id,
            start=cut.start,
            duration=cut.duration,
            channel=0,
            language=lang,
        )
    ]

    if cut.custom is not None:
        cut.custom["lang_prob"] = lang_prob
    else:
        cut.custom = {"lang_prob": lang_prob}

    return cut
