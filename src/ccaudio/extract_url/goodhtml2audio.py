from bs4 import BeautifulSoup
from urllib.parse import urljoin
import json
from argparse import ArgumentParser
import os
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed
from loguru import logger

from urllib.parse import urlparse

AUDIO_EXTENSIONS = (".mp3", ".wav", ".ogg", ".flac", ".m4a", ".aac")


def is_valid_url(url):
    """URLが有効かどうかをチェック"""
    try:
        parsed = urlparse(url)
        # 基本的な形式チェック
        if not parsed.scheme and not parsed.netloc and not parsed.path:
            return False
        return True
    except:
        return False


def safe_urljoin(base_url, url):
    """安全なurljoin - 無効なURLの場合はNoneを返す"""
    try:
        # URLが既に絶対URLの場合はそのまま返す
        if url.startswith(("http://", "https://")):
            return url if is_valid_url(url) else None

        # 相対URLの場合のみurljoinを使用
        if base_url:
            joined = urljoin(base_url, url)
            return joined if is_valid_url(joined) else None
        else:
            return url if is_valid_url(url) else None
    except Exception:
        return None


import re


def looks_like_audio_url(url):
    # 拡張子判定を厳密化（クエリやフラグメントを除く）
    clean_url = re.sub(r"[?#].*$", "", url.lower())
    return any(clean_url.endswith(ext) for ext in AUDIO_EXTENSIONS)


def extract_audio_url_pairs(html, base_url=""):
    soup = BeautifulSoup(html, "html.parser")
    pairs = []
    seen_urls = set()

    def extract_license(tag):
        for ancestor in [tag] + list(tag.parents):
            for attr in ["license", "data-license", "data-licence"]:
                if attr in ancestor.attrs:
                    return ancestor[attr]
        return ""

    def add_audio_url(tag, url, fallback_desc=""):
        audio_url = safe_urljoin(base_url, url)
        if audio_url and looks_like_audio_url(audio_url) and audio_url not in seen_urls:
            seen_urls.add(audio_url)
            description = (
                tag.get("title", "") or get_surrounding_text(tag) or fallback_desc
            )
            license_info = extract_license(tag)
            pairs.append(
                {
                    "audio_url": audio_url,
                    "description": description,
                    "license": license_info,
                }
            )

    # <audio src=...>
    for audio in soup.find_all("audio"):
        src = audio.get("src")
        if src:
            add_audio_url(audio, src)

        # <source src=...>
        for source in audio.find_all("source"):
            src = source.get("src")
            if src:
                add_audio_url(source, src, fallback_desc=get_surrounding_text(audio))

    # 一般タグ: href や src を持つタグをすべて対象に
    for tag in soup.find_all(
        ["a", "source", "embed", "iframe", "object", "track", "link"]
    ):
        for attr in ["href", "src"]:
            if attr in tag.attrs:
                add_audio_url(tag, tag[attr])

    return pairs


def get_surrounding_text(tag, max_len=100):
    # Surrounding sibling text or parent summary
    text_parts = []

    # Try next/previous siblings
    prev_text = tag.find_previous(string=True)
    next_text = tag.find_next(string=True)
    if prev_text:
        text_parts.append(prev_text.strip())
    if next_text:
        text_parts.append(next_text.strip())

    # Fallback: parent text
    if not text_parts and tag.parent:
        parent_text = tag.parent.get_text(strip=True)
        text_parts.append(parent_text)

    # Join and truncate
    combined = " ".join(text_parts).strip()
    return combined[:max_len] if combined else ""


def process_html(input_path, output_path):
    results = []
    with open(input_path, "r", encoding="utf-8") as infile:
        for i, line in tqdm(enumerate(infile), desc="Processing HTML", unit="line"):
            data = json.loads(line)
            html = data["html"]
            url = data["url"]
            pairs = extract_audio_url_pairs(html, url)
            for pair in pairs:
                audio_url = pair["audio_url"]
                description = pair["description"]
                license_info = pair.get("license", "")
                results.append(
                    {
                        "audio_url": audio_url,
                        "description": description,
                        "license": license_info,
                        "url": url,
                        "title": data["title"],
                        "quality": data["quality"],
                        "text": data["text"],
                    }
                )
    logger.info(f"Extracted {len(results)} audio URL pairs from {input_path}")
    with open(output_path, "w", encoding="utf-8") as outfile:
        for result in results:
            outfile.write(json.dumps(result, ensure_ascii=False) + "\n")


def parse_args():
    parser = ArgumentParser()
    parser.add_argument(
        "--input_dir",
        type=str,
        default="data/cc/goodhtml/2025-18",
        help="Input JSONL file path",
    )
    parser.add_argument(
        "--output_dir",
        type=str,
        default="data/cc/audio",
        help="Output directory for good HTML files",
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


if __name__ == "__main__":
    args = parse_args()
    output_dir = os.path.join(args.output_dir, os.path.basename(args.input_dir))
    os.makedirs(output_dir, exist_ok=True)
    input_paths = [
        os.path.join(args.input_dir, f)
        for f in os.listdir(args.input_dir)
        if f.endswith(".jsonl")
    ]
    input_paths = sorted(input_paths)[: args.max_num_files]
    with ProcessPoolExecutor() as executor:
        futures = []
        for input_path in input_paths:
            output_path = os.path.join(
                output_dir,
                os.path.basename(input_path),
            )
            if os.path.exists(output_path) and not args.overwrite:
                logger.info(f"File already exists: {output_path}")
                continue
            futures.append(executor.submit(process_html, input_path, output_path))

        for future in tqdm(
            as_completed(futures),
            total=len(futures),
            desc="Processing HTML",
            unit="line",
        ):
            future.result()
    logger.info(f"Processed {len(futures)} files.")
