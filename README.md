# Common Crawl Audio

- [Hugging Face](https://huggingface.co/datasets/llm-jp/cc-audio-2025-18-rss)

## Usage

```sh
cd src/ccaudio/ccaudio_downloader
uv run scrapy crawl ccaudio_spider
```

Please change `SHAR_OUTPUT_DIR` and `SHAR_SHARD_SIZE` in `src/ccaudio/ccaudio_downloader/ccaudio_downloader/settings.py`
