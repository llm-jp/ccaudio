# Common Crawl Audio

Common Crawl Audioデータセットのダウンロードと前処理を行うツールです。

## 概要

このツールは、Common Crawlから音声データを収集し、処理するためのPythonパッケージです。収集したデータは[Hugging Face](https://huggingface.co/datasets/llm-jp/cc-audio-2025-18-rss)で公開されています。

## 必要な環境

- [uv](https://github.com/astral-sh/uv) (Pythonパッケージマネージャー)
- 十分なディスクスペース（日本語のもののみダウンロードする場合は 2 TB ほど）

## セットアップ

```sh
uv sync
```

## 使い方

### 1. データのダウンロード

Hugging Faceのデータセットに保存されている音声URLから、Common Crawlから生の音声データをダウンロードします。

[lhotse](https://lhotse.readthedocs.io/en/latest/index.html)のshar形式で保存します。

```sh
cd src/ccaudio/ccaudio_downloader
uv run scrapy crawl ccaudio_spider -s SHAR_OUTPUT_DIR=/path/to/shar/dir/
```

**パラメータ：**
- `SHAR_OUTPUT_DIR`: ダウンロードした音声をshar形式で保存するディレクトリのパス

なお、本コードでは `language` カラムが `ja`, `ja_JP`, `ja-jp`, `ja-JP` のもののみをダウンロードするようにしていますが、このフィルタリングを変えたい場合は `src/ccaudio/ccaudio_downloader/ccaudio_downloader/spiders/ccaudio_spider.py` の `CcaudioSpiderSpider.start` メソッドを編集してください。

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

### 2. データの前処理

ダウンロードしたデータを処理し、使いやすい形式に変換します。前処理で行うのは以下の処理です。

- リサンプリング
- モノラル化
- [demucs](https://github.com/adefossez/demucs)によるデノイズ

```sh
uv run src/ccaudio/preprocess.py \
  --shar_dir /path/to/shar/dir \
  --output_dir /path/to/output/dir
```

**パラメータ：**
- `--shar_dir`: ダウンロードしたsharが保存されているディレクトリ
- `--output_dir`: 前処理後の音声をshar形式で保存するディレクトリ

### 3. ダウンロードしたデータの使い方

`src/ccaudio/load_shar_sample.py` を参照してください。

```sh
uv run src/ccaudio/load_shar_sample.py --shar_dir /path/to/shar/dir/
```
