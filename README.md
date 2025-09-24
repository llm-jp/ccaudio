# Common Crawl Audio

- [Hugging Face](https://huggingface.co/datasets/llm-jp/cc-audio-2025-18-rss)

## Setup

```sh
uv sync
```

## Download

```sh
cd src/ccaudio/ccaudio_downloader
uv run scrapy crawl ccaudio_spider -s SHAR_OUTPUT_DIR=/path/to/shar/dir/
```

## Preprocess

```sh
uv run src/ccaudio/preprocess.py \
  --shar_dir /path/to/shar/dir \
  --output_dir /path/to/output/dir
```
