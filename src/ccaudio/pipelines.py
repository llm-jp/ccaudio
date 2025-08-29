import os
import tempfile
import uuid
from pathlib import Path
from urllib.parse import urlparse

import lhotse
from lhotse import MonoCut, MultiCut, Recording, SupervisionSegment
from lhotse.shar import SharWriter
from pydub import AudioSegment
from scrapy.exceptions import DropItem

from ccaudio.items import AudioItem
from ccaudio.spiders import CCAudioSpider


class CCAudioPipeline(object):
    def __init__(self, output_dir: str, shard_size: int) -> None:
        self.output_dir = Path(output_dir)
        self.shard_size = shard_size
        self.writer: SharWriter | None = None

        lhotse.set_audio_duration_mismatch_tolerance(100.0)

    def open_spider(self, spider: CCAudioSpider) -> None:
        _ = spider
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.writer = SharWriter(
            str(self.output_dir),
            fields={"recording": "flac"},
            shard_size=self.shard_size,
        )
        self.writer.__enter__()

    def close_spider(self, spider: CCAudioSpider) -> None:
        _ = spider
        if self.writer:
            self.writer.__exit__(None, None, None)

    def process_item(self, item: AudioItem, spider: CCAudioSpider) -> AudioItem:
        try:
            id = uuid.uuid4().hex
            with tempfile.TemporaryDirectory() as tmp_dir:
                parsed_url = urlparse(item["audio_url"])
                filename = os.path.basename(parsed_url.path)
                if not filename:
                    filename = f"{id}.audio"

                tmp_path = Path(tmp_dir) / filename

                with open(tmp_path, "wb") as f:
                    f.write(item["audio_data"])

                _, extension = os.path.splitext(parsed_url.path)
                if not extension:
                    content_type = item.get("content_type", "")
                    if "audio/mpeg" in content_type:
                        extension = ".mp3"
                    elif "audio/wav" in content_type:
                        extension = ".wav"
                    elif "audio/flac" in content_type:
                        extension = ".flac"
                    else:
                        extension = ".mp3"

                if extension not in (".wav", ".flac", ".mp3"):
                    audio = AudioSegment.from_file(tmp_path, format=extension[1:])
                    tmp_path = tmp_path.with_suffix(".wav")
                    audio.export(tmp_path, format="wav")

                recording = Recording.from_file(
                    tmp_path, recording_id=f"recording_{id}"
                ).move_to_memory()
                assert recording.channel_ids is not None

                supervision = SupervisionSegment(
                    id=f"segment_{id}",
                    recording_id=recording.id,
                    start=0,
                    duration=recording.duration,
                    channel=recording.channel_ids,
                    language=item["language"],
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
                            "audio_url": item["audio_url"],
                            "title": item["title"],
                            "description": item["description"],
                            "page_url": item["page_url"],
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
                            "audio_url": item["audio_url"],
                            "title": item["title"],
                            "description": item["description"],
                            "page_url": item["page_url"],
                        },
                    )

                if self.writer:
                    self.writer.write(cut)

                del item["audio_data"]
                return item

        except Exception as e:
            spider.logger.warning(
                f"Error processing item {item.get('audio_url', '')}: {e}"
            )
            raise DropItem(f"Failed to process audio: {e}")
