import argparse
import os
from pathlib import Path

from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from ccaudio.download.scrapy_spider import AudioSpider


def main(output_dir: Path) -> None:
    settings = get_project_settings()

    settings_module = "ccaudio.download.scrapy_settings"
    os.environ.setdefault("SCRAPY_SETTINGS_MODULE", settings_module)

    settings.setmodule(settings_module)

    settings.set("OUTPUT_DIR", str(output_dir))

    process = CrawlerProcess(settings)
    process.crawl(AudioSpider)
    process.start()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output_dir", type=str, required=True)
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    main(output_dir)
