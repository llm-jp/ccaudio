import argparse
import shutil
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


def separate(cut: MonoCut, separator: Separator, audio_dir: Path) -> MonoCut:
    audio = torch.from_numpy(cut.load_audio())
    if audio.shape[0] == 1:
        audio = audio.repeat(2, 1)
    _, separated = separator.separate_tensor(audio)

    output_path = audio_dir / f"{cut.id}.flac"
    sf.write(
        output_path,
        separated["vocals"][0].unsqueeze(0).T,
        separator.samplerate,
        format="FLAC",
    )

    recording = Recording.from_file(output_path, recording_id=cut.recording_id)
    cut.recording = recording

    return cut


def main(shar_dir: Path, output_dir: Path) -> None:
    cut_paths = sorted(list(map(str, shar_dir.glob("cuts.*.jsonl.gz"))))
    recording_paths = sorted(list(map(str, shar_dir.glob("recording.*.tar"))))

    cuts = CutSet.from_shar({"cuts": cut_paths, "recording": recording_paths})

    separator = Separator()

    audio_dir = output_dir / "tmp"
    audio_dir.mkdir(parents=True, exist_ok=True)

    try:
        cuts = cuts.map(lambda cut: convert_audio(cut, separator.samplerate)).map(
            lambda cut: separate(cut, separator, audio_dir)
        )

        output_dir.mkdir(parents=True, exist_ok=True)

        cuts.to_shar(
            output_dir,
            fields={"recording": "flac"},
            shard_size=100,
            num_jobs=1,
            fault_tolerant=True,
            verbose=True,
        )

    finally:
        shutil.rmtree(audio_dir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--shar_dir", type=str, required=True)
    parser.add_argument("--output_dir", type=str, required=True)
    args = parser.parse_args()

    main(Path(args.shar_dir), Path(args.output_dir))
