import argparse
from pathlib import Path

from lhotse import CutSet


def main(shar_dir: Path) -> None:
    cut_paths = sorted(list(map(str, shar_dir.glob("**/cuts.*.jsonl.gz"))))
    recording_paths = sorted(list(map(str, shar_dir.glob("**/recording.*.tar"))))

    cuts = CutSet.from_shar({"cuts": cut_paths, "recording": recording_paths})

    for cut in cuts.data:
        print(cut)
        break


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--shar_dir", type=str, required=True)
    args = parser.parse_args()

    shar_dir = Path(args.shar_dir)
    main(shar_dir)
