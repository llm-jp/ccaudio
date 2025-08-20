from datasets import load_dataset


def main() -> None:
    ds = load_dataset("llm-jp/cc-audio-2025-18-rss", split="train")
    ja_items = ["ja", "ja_JP", "ja-jp", "ja-JP"]
    ds = ds.filter(lambda x: x["language"] in ja_items)
    print(ds)


if __name__ == "__main__":
    main()
