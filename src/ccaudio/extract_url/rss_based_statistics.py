from collections import Counter

import matplotlib.cm as cm
import matplotlib.pyplot as plt
from datasets import load_dataset

# font size
# plt.rcParams["font.size"] = 22


# Hubにプッシュ（プライベートリポジトリ）
ds = load_dataset("llm-jp/cc-audio-2025-18-rss", split="train")


# # dedup audio_url
# audio_urls = ds["audio_url"]
# unique_audio_urls = set(audio_urls)

# print(f"Total audio URLs: {len(audio_urls)}")
# print(f"Unique audio URLs: {len(unique_audio_urls)}")
# exit()

# ja_items = ["ja", "ja_JP", "ja-jp", "ja-JP"]
# # ja_language のみフィルタ
# ds_ja = ds.filter(lambda x: x["language"] in ja_items)

# # ドメイン抽出
# page_urls = ds_ja["audio_url"]
# domains = []
# for url in page_urls:
#     if pd.notna(url):
#         ext = tldextract.extract(url)
#         domain = ".".join(part for part in [ext.domain, ext.suffix] if part)
#         if domain:
#             domains.append(domain)
# domain_counts = Counter(domains)
# # Top 50 ドメインと割合計算
# top_k = 20
# total = sum(domain_counts.values())
# top_domains = domain_counts.most_common(top_k)
# domain_names, counts = zip(*top_domains)
# percentages = [count / total * 100 for count in counts]

# # カラーマップ設定
# norm = plt.Normalize(0, max(percentages))
# colors = cm.viridis(norm(percentages))

# # プロット作成
# fig, ax = plt.subplots(figsize=(18, 8))
# bars = ax.bar(domain_names, percentages, color=colors)
# ax.spines["right"].set_visible(False)
# ax.spines["top"].set_visible(False)

# # ラベルの回転と整形
# plt.xticks(rotation=45, ha="right")
# ax.set_ylabel("Percentage (%)")
# # ax.set_title(f"The top-{top_k} most frequent Domains", fontsize=14)

# # 値ラベル追加（小数点4桁）
# for bar, pct in zip(bars, percentages):
#     height = bar.get_height()
#     ax.text(
#         bar.get_x() + bar.get_width() / 2,
#         height + 0.002,
#         f"{pct:.2f}",
#         ha="center",
#         va="bottom",
#         fontsize=20,
#         rotation=45,
#     )

# # 軽めのyグリッド
# ax.grid(axis="y", linestyle="--", alpha=0.8)

# # レイアウトと保存
# plt.tight_layout()
# plt.savefig("audio_url_top_domains_rss_ja.png", dpi=300)
# plt.show()

plt.rcParams["font.size"] = 40
# === Language column の top-K 集計とプロット ===
language_counts = Counter(ds["language"])
top_k_lang = 15  # 表示する上位言語数

# 上位言語と割合計算
top_languages = language_counts.most_common(top_k_lang)
lang_names, lang_counts = zip(*top_languages)
total_lang = sum(language_counts.values())
lang_percentages = [count / total_lang * 100 for count in lang_counts]

# カラーマップ設定（同じ viridis）
norm_lang = plt.Normalize(0, max(lang_percentages))
lang_colors = cm.viridis(norm_lang(lang_percentages))

# プロット作成
fig, ax = plt.subplots(figsize=(18, 8))
bars = ax.bar(lang_names, lang_percentages, color=lang_colors)
ax.spines["right"].set_visible(False)
ax.spines["top"].set_visible(False)

# ラベルの回転と整形
plt.xticks(rotation=45, ha="right")
ax.set_ylabel("Percentage (%)")
# ax.set_title(f"Top-{top_k_lang} Languages", fontsize=14)

# 値ラベル追加（小数点2桁）
for bar, pct in zip(bars, lang_percentages):
    height = bar.get_height()
    ax.text(
        bar.get_x() + bar.get_width() / 2,
        height + 0.002,
        f"{pct:.2f}",
        ha="center",
        va="bottom",
        fontsize=35,
        rotation=45,
    )

# y軸グリッド
ax.grid(axis="y", linestyle="--", alpha=0.8)

# レイアウトと保存
plt.tight_layout()
plt.savefig("audio_language_distribution_ja.png", dpi=300)
plt.show()
