import argparse
from pathlib import Path

from lhotse import CutSet

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--shar_dir", type=str, required=True)
    args = parser.parse_args()

    cut_paths = sorted(list(map(str, Path(args.shar_dir).glob("cuts.*.jsonl.gz"))))
    recording_paths = sorted(
        list(map(str, Path(args.shar_dir).glob("recording.*.tar")))
    )

    cuts = CutSet.from_shar({"cuts": cut_paths, "recording": recording_paths})
    print(cuts)

    for cut in cuts.data:
        print(cut)
        wav = cut.load_audio()
        print(wav.shape)
        break
