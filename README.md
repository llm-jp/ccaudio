# Common Crawl Audio

- [Hugging Face](https://huggingface.co/datasets/llm-jp/cc-audio-2025-18-rss)

## Download

```sh
cd src/ccaudio/ccaudio_downloader
uv run scrapy crawl ccaudio_spider
```

Please change `SHAR_OUTPUT_DIR` in `src/ccaudio/ccaudio_downloader/ccaudio_downloader/settings.py`

## Preprocess

```sh
uv run src/ccaudio/preprocess/preprocess.py --download_dir $DOWNLOAD_DIR --output_dir $OUTPUT_DIR
```
