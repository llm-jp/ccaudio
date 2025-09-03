# Common Crawl Audio

- [Hugging Face](https://huggingface.co/datasets/llm-jp/cc-audio-2025-18-rss)

## Download

```sh
uv run src/ccaudio/download/main.py --download_dir $DOWNLOAD_DIR
```

## Preprocess

```sh
uv run src/ccaudio/preprocess/preprocess.py --download_dir $DOWNLOAD_DIR --output_dir $OUTPUT_DIR
```
