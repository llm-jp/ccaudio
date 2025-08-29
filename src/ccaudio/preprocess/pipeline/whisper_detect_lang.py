import numpy as np
from faster_whisper import WhisperModel
from lhotse import MonoCut


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

    s = cut.supervisions[0]
    s.language = lang
    if s.custom is not None:
        s.custom["lang_prob"] = lang_prob
    else:
        s.custom = {"lang_prob": lang_prob}
    cut.supervisions = [s]

    return cut
