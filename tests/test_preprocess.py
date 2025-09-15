from pathlib import Path

from demucs.api import Separator
from lhotse import CutSet, MonoCut, MultiCut

from ccaudio.preprocess import convert_audio, separate


def test_preprocess() -> None:
    separator = Separator()

    shar_dir = Path("/groups/gcg51557/experiments/0167_cc_audio/asai/ccaudio_raw/")

    cut_paths = sorted(list(map(str, shar_dir.glob("cuts.*.jsonl.gz"))))
    recording_paths = sorted(list(map(str, shar_dir.glob("recording.*.tar"))))

    cuts = CutSet.from_shar({"cuts": cut_paths, "recording": recording_paths})

    for cut in cuts.data:
        assert isinstance(cut, MonoCut) or isinstance(cut, MultiCut)
        c = convert_audio(cut, separator.samplerate)
        c = separate(c, separator)

        break
