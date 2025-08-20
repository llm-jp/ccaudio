import os
import tempfile
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from logging import INFO, WARNING, Formatter, StreamHandler, getLogger
from pathlib import Path
from typing import Any, Generator
from urllib.parse import urlparse

from datasets.arrow_dataset import Dataset
from lhotse import MonoCut, MultiCut, Recording, SupervisionSegment
from lhotse.cut import Cut
from tqdm import tqdm

from ccaudio.download.util import download_file


class Downloader:
    def __init__(self, ds: Dataset, max_workers: int = 16) -> None:
        self.ds = ds
        self.max_workers = max_workers

        self.logger = getLogger(self.__module__)
        self.logger.setLevel(WARNING)
        ch = StreamHandler()
        ch.setLevel(INFO)
        ch_formatter = Formatter(
            "[%(asctime)s][%(name)s][%(levelname)s](%(filename)s:%(lineno)s) %(message)s"
        )
        ch.setFormatter(ch_formatter)

        if len(self.logger.handlers) == 0:
            self.logger.addHandler(ch)

    def _process_single_item(self, data: Any, tmp_dir: str) -> Cut | None:
        try:
            assert isinstance(data, dict)

            parsed_url = urlparse(data["audio_url"])
            filename = os.path.basename(parsed_url.path)

            tmp_path = Path(tmp_dir) / filename
            download_file(data["audio_url"], tmp_path, progress_bar=False)

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

            return cut

        except Exception as e:
            self.logger.warning(e)
            return None

    def get_cuts(self) -> Generator[Cut, None, None]:
        with tempfile.TemporaryDirectory() as tmp_dir:
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = {
                    executor.submit(self._process_single_item, data, tmp_dir)
                    for data in self.ds
                }

                for future in tqdm(as_completed(futures), total=len(self.ds)):
                    result = future.result()
                    if result is not None:
                        yield result
