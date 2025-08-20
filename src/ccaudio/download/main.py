import os
import tempfile
from pathlib import Path
from urllib.parse import urlparse

from datasets import load_dataset
from tqdm import tqdm

from ccaudio.download.util import download_file


def main() -> None:
    ds = load_dataset("llm-jp/cc-audio-2025-18-rss", split="train")
    ja_items = ["ja", "ja_JP", "ja-jp", "ja-JP"]
    ds = ds.filter(lambda x: x["language"] in ja_items)

    with tempfile.TemporaryDirectory() as tmp_dir:
        for data in tqdm(ds):
            parsed_url = urlparse(data["audio_url"])
            filename = os.path.basename(parsed_url.path)

            tmp_path = Path(tmp_dir) / filename
            try:
                download_file(data["audio_url"], tmp_path)
            except Exception as e:
                print(e)
                continue

            break


if __name__ == "__main__":
    main()
