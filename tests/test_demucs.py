from pathlib import Path

import torch
import torchaudio
from demucs.api import Separator
from lhotse import CutSet


def test_demucs() -> None:
    separator = Separator()

    shar_dir = Path("/groups/gcg51557/experiments/0167_cc_audio/asai/ccaudio_raw/")

    cut_paths = sorted(list(map(str, shar_dir.glob("cuts.*.jsonl.gz"))))
    recording_paths = sorted(list(map(str, shar_dir.glob("recording.*.tar"))))

    cuts = CutSet.from_shar({"cuts": cut_paths, "recording": recording_paths})

    for cut in cuts.data:
        audio = torch.from_numpy(cut.load_audio())
        origin, separated = separator.separate_tensor(audio)

        torchaudio.save("origin.wav", origin, cut.sampling_rate)

        for k, v in separated.items():
            torchaudio.save(f"{k}.wav", v, cut.sampling_rate)

        break
