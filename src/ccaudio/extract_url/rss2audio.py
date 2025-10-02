from warcio.archiveiterator import ArchiveIterator
from bs4 import BeautifulSoup
import re
import requests
from argparse import ArgumentParser
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm
from loguru import logger
import json


def is_rss_feed(http_headers, payload):
    """RSS/Atomかどうか判定"""
    ctype = http_headers.get_header("Content-Type") or ""
    if "rss+xml" in ctype or "atom+xml" in ctype or "xml" in ctype:
        return True
    if (
        payload.strip().startswith(b"<?xml")
        or payload.strip().startswith(b"<rss")
        or payload.strip().startswith(b"<feed")
    ):
        return True
    return False


def extract_audio_urls_from_rss(xml_content):
    """RSS XMLから音声URL・タイトル・説明を<item>ごとに抽出"""
    soup = BeautifulSoup(xml_content, "xml")
    entries = []

    channel = soup.find("channel")
    language_tag = channel.find("language") if channel else None
    language = language_tag.text.strip() if language_tag else None

    for item in soup.find_all("item"):
        # 音声URLの候補を探す
        url = None

        enclosure = item.find("enclosure")
        if enclosure and enclosure.get("url"):
            url = enclosure.get("url")
        else:
            media = item.find("media:content")
            if media and media.get("url"):
                url = media.get("url")

        if url and re.search(r"\.(mp3|m4a|aac|wav|ogg|flac)$", url):
            title = item.find("title")
            description = item.find("description")

            entries.append(
                {
                    "audio_url": url,
                    "title": title.text.strip() if title else "",
                    "description": description.text.strip() if description else "",
                    "language": language if language else "",
                }
            )

    return entries


def extract_from_warc(input_path, output_path):
    """WARCからRSSフィードを探し音声URLを抽出"""
    results = []
    with requests.get(input_path, stream=True) as r:
        r.raise_for_status()
        for record in ArchiveIterator(r.raw):
            if record.rec_type != "response":
                continue
            http_headers = record.http_headers
            payload = record.content_stream().read()

            if not is_rss_feed(http_headers, payload):
                continue

            try:
                entries = extract_audio_urls_from_rss(payload)
                if entries:
                    page_url = record.rec_headers.get_header("WARC-Target-URI")

                    for entry in entries:
                        results.append(
                            {
                                "audio_url": entry["audio_url"],
                                "title": entry["title"],
                                "description": entry["description"],
                                "language": entry["language"],
                                "page_url": page_url,
                            }
                        )
            except Exception as e:
                print(f"[!] Parse error: {e}")
    with open(output_path, "w", encoding="utf-8") as outfile:
        for result in results:
            outfile.write(json.dumps(result, ensure_ascii=False) + "\n")


def parse_args():
    parser = ArgumentParser()
    parser.add_argument(
        "--input_file",
        type=str,
        default="data/cc/url/2025-18.txt",
        help="File containing urls of warc files",
    )
    parser.add_argument(
        "--output_dir",
        type=str,
        default="data/cc/audio",
        help="Output directory for JSONL files",
    )
    parser.add_argument(
        "--max_num_files",
        type=int,
        default=None,
        help="Maximum number of WARC files to process",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing files",
    )

    return parser.parse_args()


# 使用例
if __name__ == "__main__":
    args = parse_args()
    # wandb.init("cc-audio")
    output_dir = os.path.join(
        args.output_dir, os.path.basename(args.input_file).split(".")[0]
    )
    os.makedirs(output_dir, exist_ok=True)
    with open(args.input_file, "r") as f:
        input_paths = f.readlines()
    input_paths = [url.strip() for url in input_paths]
    warc_urls = sorted(input_paths)
    input_paths = input_paths[: args.max_num_files]

    with ProcessPoolExecutor() as executor:
        futures = []
        for input_path in input_paths:
            output_path = os.path.join(
                output_dir,
                os.path.basename(input_path).replace(".warc.gz", ".jsonl"),
            )
            if os.path.exists(output_path) and not args.overwrite:
                logger.info(f"File already exists: {output_path}")
                continue
            futures.append(executor.submit(extract_from_warc, input_path, output_path))

        for future in tqdm(
            as_completed(futures),
            total=len(futures),
            desc="Processing HTML",
            unit="line",
        ):
            future.result()
    logger.info(f"Processed {len(futures)} files.")
