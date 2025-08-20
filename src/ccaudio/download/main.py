import argparse
from pathlib import Path

from datasets import load_dataset
from datasets.arrow_dataset import Dataset
from lhotse.shar import SharWriter

from ccaudio.download.downloader import Downloader


def main(output_dir: Path, max_workers: int) -> None:
    ds = load_dataset("llm-jp/cc-audio-2025-18-rss", split="train")
    ja_items = ["ja", "ja_JP", "ja-jp", "ja-JP"]
    ds = ds.filter(lambda x: x["language"] in ja_items)

    assert isinstance(ds, Dataset)

    downloader = Downloader(ds, max_workers=max_workers)

    output_dir.mkdir(parents=True, exist_ok=True)

    with SharWriter(
        str(output_dir), fields={"recording": "flac"}, shard_size=10
    ) as writer:
        for cut in downloader.get_cuts():
            writer.write(cut)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output_dir", type=str, required=True)
    parser.add_argument("--max_workers", type=int, default=16, required=False)
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    main(output_dir, args.max_workers)
