from collections import Counter

import matplotlib.cm as cm
import matplotlib.pyplot as plt
import pandas as pd
import tldextract
from datasets import load_dataset

# font size
plt.rcParams["font.size"] = 22

# # データ読み込み
# data_dir = "data/cc/audio/2025-18-parquet"
# df = pd.concat(
#     [
#         pd.read_parquet(os.path.join(data_dir, f))
#         for f in os.listdir(data_dir)
#         if f.endswith(".parquet")
#     ],
#     ignore_index=True,
# )


# print(f"Loaded {len(df)} records from {data_dir}")
# df = df.drop_duplicates(subset=["audio_url"], keep="first")
# print(f"After deduplication, {len(df)} records remain")

# ds = datasets.Dataset.from_pandas(df, preserve_index=False)
# print(ds)
# ds = ds.remove_columns(["license"])
# ds.push_to_hub("llm-jp/cc-audio-2025-18", private=True)
# exit()

ds = load_dataset("llm-jp/cc-audio-2025-18", split="train")

# ドメイン抽出
page_urls = ds["audio_url"]
domains = []
for url in page_urls:
    if pd.notna(url):
        ext = tldextract.extract(url)
        domain = ".".join(part for part in [ext.domain, ext.suffix] if part)
        if domain:
            domains.append(domain)
domain_counts = Counter(domains)
# Top 50 ドメインと割合計算
top_k = 20
total = sum(domain_counts.values())
top_domains = domain_counts.most_common(top_k)
domain_names, counts = zip(*top_domains)
percentages = [count / total * 100 for count in counts]

# カラーマップ設定
norm = plt.Normalize(0, max(percentages))
colors = cm.viridis(norm(percentages))

# プロット作成
fig, ax = plt.subplots(figsize=(18, 8))
bars = ax.bar(domain_names, percentages, color=colors)
ax.spines["right"].set_visible(False)
ax.spines["top"].set_visible(False)

# ラベルの回転と整形
plt.xticks(rotation=45, ha="right")
ax.set_ylabel("Percentage (%)")
# ax.set_title(f"The top-{top_k} most frequent Domains", fontsize=14)

# 値ラベル追加（小数点4桁）
for bar, pct in zip(bars, percentages):
    height = bar.get_height()
    ax.text(
        bar.get_x() + bar.get_width() / 2,
        height + 0.002,
        f"{pct:.2f}",
        ha="center",
        va="bottom",
        rotation=45,
        fontsize=20,
    )

# 軽めのyグリッド
ax.grid(axis="y", linestyle="--", alpha=0.8)

# レイアウトと保存
plt.tight_layout()
plt.savefig("audio_url_top_domains.png", dpi=300)
plt.show()
