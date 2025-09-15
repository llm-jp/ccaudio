import io
import logging
import os
import tempfile
from pathlib import Path
from urllib.parse import urlparse

import soundfile as sf
from faster_whisper import WhisperModel
from itemadapter import ItemAdapter
from lhotse import MonoCut, MultiCut, Recording
from lhotse.shar import SharWriter
from pydub import AudioSegment

logger = logging.getLogger(__name__)


class LhotseSharPipeline:
    """Pipeline to save audio data in Lhotse shar format"""

    def __init__(
        self,
        output_dir: str = "output",
        shard_size: int = 5000,
        preprocess: bool = False,
    ):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.shard_size = shard_size
        self.preprocess = preprocess
        self.writer = None
        self.cuts = []
        self.item_count = 0
        self.model = WhisperModel(model_size_or_path="large-v3")

    @classmethod
    def from_crawler(cls, crawler):
        """Create pipeline from crawler settings"""
        output_dir = crawler.settings.get("SHAR_OUTPUT_DIR", "output")
        shard_size = crawler.settings.getint("SHAR_SHARD_SIZE", 5000)
        return cls(output_dir=output_dir, shard_size=shard_size)

    def open_spider(self, spider):
        """Initialize shar writer when spider opens"""
        shar_path = self.output_dir
        logger.info(f"Opening SharWriter at {shar_path}")
        self.writer = SharWriter(
            output_dir=str(shar_path),
            fields={"recording": "flac"},
            shard_size=self.shard_size,
            warn_unused_fields=False,
        )
        self.writer.__enter__()

    def close_spider(self, spider):
        """Close shar writer when spider closes"""
        if self.writer:
            self.writer.close()
            logger.info(f"Closed SharWriter. Total items processed: {self.item_count}")

    def _get_audio_format(self, item: dict) -> str:
        """Determine audio format from content type or URL"""
        content_type = item.get("content_type", "")
        audio_url = item.get("audio_url", "")

        # Check content type first
        if "audio/mpeg" in content_type or "audio/mp3" in content_type:
            return "mp3"
        elif "audio/wav" in content_type or "audio/wave" in content_type:
            return "wav"
        elif "audio/flac" in content_type:
            return "flac"
        elif "audio/ogg" in content_type:
            return "ogg"

        # Fall back to URL extension
        parsed_url = urlparse(audio_url)
        path = parsed_url.path.lower()

        if path.endswith(".mp3"):
            return "mp3"
        elif path.endswith(".wav"):
            return "wav"
        elif path.endswith(".flac"):
            return "flac"
        elif path.endswith(".ogg"):
            return "ogg"
        elif path.endswith(".m4a"):
            return "m4a"

        # Default to mp3 if unknown
        return "mp3"

    def _convert_to_wav(self, audio_data: bytes, input_format: str) -> bytes:
        """Convert audio to WAV format using pydub"""
        try:
            # Load audio from bytes
            audio = AudioSegment.from_file(io.BytesIO(audio_data), format=input_format)

            # Export as WAV
            wav_buffer = io.BytesIO()
            audio.export(wav_buffer, format="wav")
            wav_buffer.seek(0)

            return wav_buffer.read()
        except Exception as e:
            logger.error(f"Failed to convert audio from {input_format} to WAV: {e}")
            raise

    def process_item(self, item, spider):
        """Process audio item and save to Lhotse shar format"""
        adapter = ItemAdapter(item)

        audio_data = adapter.get("audio_data")
        if not audio_data:
            logger.warning("No audio data in item, skipping")
            return item

        try:
            # Determine audio format
            audio_format = self._get_audio_format(dict(item))

            # Save audio to temporary file
            with tempfile.NamedTemporaryFile(
                suffix=f".{audio_format}", delete=False
            ) as tmp_file:
                tmp_file.write(audio_data)
                tmp_path = tmp_file.name

            try:
                # Try to read with soundfile
                info = sf.info(tmp_path)
                recording = Recording.from_file(tmp_path)
            except Exception as e:
                # If soundfile fails, convert to WAV
                logger.warning(
                    f"Failed to read {audio_format} with soundfile, converting to WAV: {e}"
                )

                # Convert to WAV
                wav_data = self._convert_to_wav(audio_data, audio_format)

                # Save WAV to new temp file
                os.unlink(tmp_path)
                with tempfile.NamedTemporaryFile(
                    suffix=".wav", delete=False
                ) as tmp_file:
                    tmp_file.write(wav_data)
                    tmp_path = tmp_file.name

                # Read the WAV file
                info = sf.info(tmp_path)
                recording = Recording.from_file(tmp_path)

            # Create a unique ID for this recording
            recording_id = f"audio_{self.item_count:08d}"
            recording.id = recording_id

            assert recording.channel_ids is not None

            # Create Cut object based on number of channels
            if recording.num_channels == 1:
                cut = MonoCut(
                    id=recording_id,
                    start=0,
                    duration=recording.duration,
                    channel=0,
                    recording=recording,
                    custom={
                        "audio_url": adapter.get("audio_url", ""),
                        "title": adapter.get("title", ""),
                        "description": adapter.get("description", ""),
                        "page_url": adapter.get("page_url", ""),
                        "language": adapter.get("language", ""),
                    },
                )
            else:
                cut = MultiCut(
                    id=recording_id,
                    start=0,
                    duration=recording.duration,
                    channel=recording.channel_ids,
                    recording=recording,
                    custom={
                        "audio_url": adapter.get("audio_url", ""),
                        "title": adapter.get("title", ""),
                        "description": adapter.get("description", ""),
                        "page_url": adapter.get("page_url", ""),
                        "language": adapter.get("language", ""),
                    },
                )
            assert self.writer is not None
            self.writer.write(cut)

            self.item_count += 1

            logger.info(
                f"Saved audio {self.item_count}: {adapter.get('title', '')[:50]}..."
            )

            # Clean up temp file
            os.unlink(tmp_path)

        except Exception as e:
            logger.error(f"Failed to process audio item: {e}")
            if "tmp_path" in locals() and os.path.exists(tmp_path):
                os.unlink(tmp_path)

        return item
