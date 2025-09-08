import io
import logging
import os
import tempfile
from dataclasses import asdict, is_dataclass
from pathlib import Path
from urllib.parse import urlparse

import numpy as np
import soundfile as sf
from faster_whisper import WhisperModel
from itemadapter import ItemAdapter
from lhotse import CutSet, MonoCut, MultiCut, Recording, SupervisionSegment
from lhotse.cut.data import DataCut
from lhotse.shar import SharWriter
from lhotse.supervision import AlignmentItem
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
        preprocess = crawler.settings.getint("PREPROCESS", False)
        return cls(output_dir=output_dir, shard_size=shard_size, preprocess=preprocess)

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

            if self.preprocess:
                cutset = self.preprocess_cut(cut)
                for c in cutset.data:
                    self.writer.write(c)
            else:
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

    def preprocess_cut(self, cut: MonoCut | MultiCut) -> CutSet:
        if isinstance(cut, MultiCut):
            mono_cut = cut.to_mono(mono_downmix=True)
            assert isinstance(mono_cut, DataCut)
        else:
            mono_cut = cut

        resampled_cut = mono_cut.resample(16000)
        assert isinstance(resampled_cut, MonoCut)

        cutset = resampled_cut.cut_into_windows(duration=30)
        assert isinstance(cutset, CutSet)

        cutset = (
            cutset.map(self.whisper_detect_lang)
            .filter(self.filter_lang_prob)
            .map(self.whisper_transcribe)
            .trim_to_alignments(
                type="word",
                keep_all_channels=True,
                num_jobs=4,
            )
        )

        return cutset

    def whisper_detect_lang(self, cut: MonoCut) -> MonoCut:
        audio = cut.load_audio()
        assert isinstance(audio, np.ndarray)

        features = self.model.feature_extractor(audio, chunk_length=30)

        lang, lang_prob, _ = self.model.detect_language(
            features=features,
            vad_filter=True,
            language_detection_segments=7,
            language_detection_threshold=0.5,
        )

        cut.supervisions = [
            SupervisionSegment(
                id=f"segment_{cut.id}",
                recording_id=cut.recording_id,
                start=cut.start,
                duration=cut.duration,
                channel=0,
                language=lang,
            )
        ]

        if cut.custom is not None:
            cut.custom["lang_prob"] = lang_prob
        else:
            cut.custom = {"lang_prob": lang_prob}

        return cut

    def filter_lang_prob(self, cut: MonoCut) -> bool:
        assert cut.custom is not None
        return cut.custom["lang_prob"] >= 0.7

    def whisper_transcribe(self, cut: MonoCut) -> MonoCut:
        audio = cut.load_audio()
        assert isinstance(audio, np.ndarray)

        segments, _ = self.model.transcribe(
            audio=audio[0], language=cut.supervisions[0].language
        )

        pred_text = ""
        alignment_items = []
        for segment in segments:
            seg = serialize(segment)

            pred_text += str(seg["text"]).strip()  # type: ignore
            alignment_items.append(
                AlignmentItem(
                    symbol=str(seg["text"]),  # type: ignore
                    start=cut.start + float(seg["start"]),  # type: ignore
                    duration=float(seg["end"]) - float(seg["start"]),  # type: ignore
                )
            )

        s = cut.supervisions[0]
        s.text = pred_text
        s.alignment = {"word": alignment_items}
        cut.supervisions = [s]

        return cut


def serialize(obj):
    if is_dataclass(obj) and not isinstance(obj, type):
        return asdict(obj)
    elif isinstance(obj, list):
        return [serialize(item) for item in obj]
    elif isinstance(obj, dict):
        return {k: serialize(v) for k, v in obj.items()}
    return obj
