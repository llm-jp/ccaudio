import argparse
from pathlib import Path

from faster_whisper import WhisperModel
from lhotse import CutSet

from ccaudio.preprocess.convert_audio import convert_audio
from ccaudio.preprocess.filter_lang_prob import filter_lang_prob
from ccaudio.preprocess.whisper_detect_lang import whisper_detect_lang
from ccaudio.preprocess.whisper_transcribe import whisper_transcribe


def main(shar_dir: Path, output_dir: Path, num_jobs: int) -> None:
    cut_paths = sorted(list(map(str, shar_dir.glob("**/cuts.*.jsonl.gz"))))
    recording_paths = sorted(list(map(str, shar_dir.glob("**/recording.*.tar"))))

    cuts = CutSet.from_shar({"cuts": cut_paths, "recording": recording_paths})

    model = WhisperModel(model_size_or_path="large-v3")

    cuts = (
        cuts.map(convert_audio)
        .cut_into_windows(
            duration=60, keep_excessive_supervisions=False, num_jobs=num_jobs
        )
        .map(lambda c: whisper_detect_lang(c, model))
        .filter(filter_lang_prob)
        .map(lambda c: whisper_transcribe(c, model))
        .trim_to_alignments(
            type="word",
            max_segment_duration=1.0,
            keep_all_channels=True,
            num_jobs=num_jobs,
        )
    )

    cuts.to_shar(
        output_dir,
        fields={"recording": "flac"},
        shard_size=5000,
        num_jobs=num_jobs,
        fault_tolerant=True,
        verbose=True,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--download_dir", type=str, required=True)
    parser.add_argument("--output_dir", type=str, required=True)
    parser.add_argument("--num_jobs", type=int, default=1, required=False)
    args = parser.parse_args()

    shar_dir = Path(args.download_dir)
    output_dir = Path(args.output_dir)
    main(shar_dir, output_dir, args.num_jobs)
