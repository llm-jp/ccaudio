# Common Crawl Audio

Tools for downloading and preprocessing the Common Crawl Audio dataset.

## Overview

This is a Python package for collecting and processing audio data from Common Crawl. The collected data is published on [Hugging Face](https://huggingface.co/datasets/llm-jp/cc-audio-2025-18-rss).

## Requirements

- [uv](https://github.com/astral-sh/uv) (Python package manager)
- Sufficient disk space (approximately 2 TB for Japanese audio only)

## Setup

```sh
uv sync
```

## Usage

### 1. Data Download

Download raw audio data from Common Crawl using the audio URLs stored in the Hugging Face dataset.

The data is saved in [lhotse](https://lhotse.readthedocs.io/en/latest/index.html) shar format.

```sh
cd src/ccaudio/ccaudio_downloader
uv run scrapy crawl ccaudio_spider -s SHAR_OUTPUT_DIR=/path/to/shar/dir/
```

**Parameters:**
- `SHAR_OUTPUT_DIR`: Directory path to save downloaded audio in shar format

Note: This code is configured to download only items where the `language` column is `ja`, `ja_JP`, `ja-jp`, or `ja-JP`. To change this filtering, edit the `LANGUAGE_ITEMS` setting in [settings.py](https://github.com/llm-jp/ccaudio/blob/main/src/ccaudio/ccaudio_downloader/ccaudio_downloader/settings.py):

```python
# Dataset settings
DATASET_NAME = "llm-jp/cc-audio-2025-18-rss"

# Set LANGUAGE_ITEMS=[] if you don't want to filter by language
LANGUAGE_ITEMS = ["ja", "ja_JP", "ja-jp", "ja-JP"]
```

### 2. Data Preprocessing

Process the downloaded data and convert it to a more usable format. The preprocessing includes:

- Resampling
- Converting to mono
- Denoising with [demucs](https://github.com/adefossez/demucs)

```sh
uv run src/ccaudio/preprocess.py \
  --shar_dir /path/to/shar/dir \
  --output_dir /path/to/output/dir
```

**Parameters:**
- `--shar_dir`: Directory containing the downloaded shar files
- `--output_dir`: Directory to save preprocessed audio in shar format

### 3. Using the Downloaded Data

See [load_shar_sample.py](https://github.com/llm-jp/ccaudio/blob/main/src/ccaudio/load_shar_sample.py) for reference.

```sh
uv run src/ccaudio/load_shar_sample.py --shar_dir /path/to/shar/dir/
```

## Citation

If you use this dataset or tools in your research, please cite:

```bibtex
@inproceedings{ccaudio2025,
  author    = {淺井 航平 and 杉浦 一瑳 and 中田 亘 and 栗田 修平 and 高道 慎之介 and 小川 哲司 and 東中 竜一郎},
  title     = {Common Crawlを用いた大規模音声音響データセットの構築},
  booktitle = {日本音響学会2025年秋季研究発表会},
  month     = {Sep.},
  year      = {2025}
}
```
