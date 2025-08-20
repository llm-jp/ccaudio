import os
import tempfile
import uuid
from logging import INFO, WARNING, Formatter, StreamHandler, getLogger
from pathlib import Path
from typing import Generator
from urllib.parse import urlparse

from datasets.arrow_dataset import Dataset
from lhotse import MonoCut, MultiCut, Recording, SupervisionSegment
from lhotse.cut import Cut
from tqdm import tqdm

from ccaudio.download.util import download_file


class Downloader:
    def __init__(self, ds: Dataset) -> None:
        self.ds = ds

        self.logger = getLogger("norfolk")
        self.logger.setLevel(WARNING)
        ch = StreamHandler()
        ch.setLevel(INFO)
        ch_formatter = Formatter(
            "[%(asctime)s][%(name)s][%(levelname)s](%(filename)s:%(lineno)s) %(message)s"
        )
        ch.setFormatter(ch_formatter)

        if len(self.logger.handlers) == 0:
            self.logger.addHandler(ch)

    def get_cuts(self) -> Generator[Cut, None, None]:
        with tempfile.TemporaryDirectory() as tmp_dir:
            for data in tqdm(self.ds):
                assert isinstance(data, dict)

                parsed_url = urlparse(data["audio_url"])
                filename = os.path.basename(parsed_url.path)

                tmp_path = Path(tmp_dir) / filename
                try:
                    download_file(data["audio_url"], tmp_path)
                except Exception as e:
                    self.logger.warning(e)
                    continue

                id = uuid.uuid4().hex

                recording = Recording.from_file(
                    tmp_path, recording_id=f"recording_{id}"
                )
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

                yield cut
