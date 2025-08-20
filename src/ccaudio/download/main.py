import os
import tempfile
import uuid
from pathlib import Path
from urllib.parse import urlparse

from datasets import load_dataset
from lhotse import MonoCut, MultiCut, Recording
from lhotse.manipulation import SupervisionSegment
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

            id = uuid.uuid4().hex

            recording = Recording.from_file(tmp_path, recording_id=f"recording_{id}")
            assert recording.channel_ids is not None

            supervision = SupervisionSegment(
                id=f"segment_{id}",
                recording_id=recording.id,
                start=0,
                duration=recording.duration,
                channel=recording.channel_ids,
                language=data["language"],
            )

            if len(recording.channel_ids) == 1:
                cut = MonoCut(
                    id=id,
                    start=0,
                    duration=recording.duration,
                    channel=0,
                    supervisions=[supervision],
                    recording=recording,
                    custom={
                        "audio_url": data["audio_url"],
                        "title": data["title"],
                        "description": data["description"],
                        "page_url": data["page_url"],
                    },
                )
            else:
                cut = MultiCut(
                    id=id,
                    start=0,
                    duration=recording.duration,
                    channel=recording.channel_ids,
                    supervisions=[supervision],
                    recording=recording,
                    custom={
                        "audio_url": data["audio_url"],
                        "title": data["title"],
                        "description": data["description"],
                        "page_url": data["page_url"],
                    },
                )

            print(cut)

            break


if __name__ == "__main__":
    main()
