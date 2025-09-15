import argparse
import io
from pathlib import Path
from typing import Union

import soundfile as sf
import torch
from demucs.api import Separator
from lhotse import CutSet, MonoCut, MultiCut, Recording
from lhotse.cut.data import DataCut


def convert_audio(cut: Union[MonoCut, MultiCut], sr: int) -> MonoCut:
    if isinstance(cut, MultiCut):
        mono_cut = cut.to_mono(mono_downmix=True)
        assert isinstance(mono_cut, DataCut)
    else:
        mono_cut = cut

    resampled_cut = mono_cut.resample(sr)
    assert isinstance(resampled_cut, MonoCut)

    return resampled_cut


def separate(cut: MonoCut, separator: Separator) -> MonoCut:
    audio = torch.from_numpy(cut.load_audio())
    if audio.shape[0] == 1:
        audio = audio.repeat(2, 1)
    _, separated = separator.separate_tensor(audio)

    buf = io.BytesIO()
    sf.write(
        buf, separated["vocals"][0].unsqueeze(0).T, separator.samplerate, format="WAV"
    )

    recording = Recording.from_bytes(buf.getvalue(), recording_id=cut.recording_id)
    cut.recording = recording

    return cut


def main(shar_dir: Path, output_dir: Path) -> None:
    cut_paths = sorted(list(map(str, shar_dir.glob("cuts.*.jsonl.gz"))))
    recording_paths = sorted(list(map(str, shar_dir.glob("recording.*.tar"))))

    cuts = CutSet.from_shar({"cuts": cut_paths, "recording": recording_paths})

    separator = Separator()

    cuts = cuts.map(lambda cut: convert_audio(cut, separator.samplerate)).map(
        lambda cut: separate(cut, separator)
    )

    output_dir.mkdir(parents=True, exist_ok=True)

    cuts.to_shar(
        output_dir,
        fields={"recording": "flac"},
        shard_size=100,
        num_jobs=4,
        fault_tolerant=True,
        verbose=True,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--shar_dir", type=str, required=True)
    parser.add_argument("--output_dir", type=str, required=True)
    args = parser.parse_args()

    main(Path(args.shar_dir), Path(args.output_dir))
