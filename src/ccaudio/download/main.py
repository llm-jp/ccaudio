from datasets import load_dataset
from datasets.arrow_dataset import Dataset

from ccaudio.download.downloader import Downloader


def main() -> None:
    ds = load_dataset("llm-jp/cc-audio-2025-18-rss", split="train")
    ja_items = ["ja", "ja_JP", "ja-jp", "ja-JP"]
    ds = ds.filter(lambda x: x["language"] in ja_items)

    assert isinstance(ds, Dataset)

    downloader = Downloader(ds)

    for cut in downloader.get_cuts():
        print(cut)
        break


if __name__ == "__main__":
    main()
