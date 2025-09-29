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

Note: This code is configured to download only items where the `language` column is `ja`, `ja_JP`, `ja-jp`, or `ja-JP`. To change this filtering, edit the `CcaudioSpiderSpider.start` method in [ccaudio_spider.py](https://github.com/llm-jp/ccaudio/blob/main/src/ccaudio/ccaudio_downloader/ccaudio_downloader/spiders/ccaudio_spider.py):

```python
async def start(self):
    """Load HuggingFace dataset and yield requests for each audio URL"""
    logger.info("Loading cc-audio-2025-18-rss dataset from HuggingFace...")

    # Load the dataset
    self.dataset = load_dataset("llm-jp/cc-audio-2025-18-rss", split="train")

    # Filter for Japanese content
    ja_items = ["ja", "ja_JP", "ja-jp", "ja-JP"]
    self.dataset = self.dataset.filter(lambda x: x["language"] in ja_items)
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
