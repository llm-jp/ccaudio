import glob
import os
from concurrent.futures import ProcessPoolExecutor

import pandas as pd
from tqdm import tqdm

# 入力と出力ディレクトリ
input_files = sorted(glob.glob("data/cc/audio/2025-18/*.jsonl"))
output_dir = "data/cc/audio/2025-18-parquet"
os.makedirs(output_dir, exist_ok=True)

files_per_group = 100  # 100ファイル単位で処理
num_workers = 8  # 並列数

# グループ単位で分割
file_groups = [
    input_files[i : i + files_per_group]
    for i in range(0, len(input_files), files_per_group)
]


def process_group(group_idx_and_files):
    group_idx, files = group_idx_and_files
    dfs = []
    for file in files:
        try:
            df = pd.read_json(file, lines=True, dtype=False)
            dfs.append(df)
        except Exception as e:
            print(f"[Group {group_idx}] Error in {file}: {e}")
    if dfs:
        df_all = pd.concat(dfs, ignore_index=True)
        out_path = os.path.join(output_dir, f"group_{group_idx:05d}.parquet")

        df_all.to_parquet(out_path, index=False)


# 並列実行
with ProcessPoolExecutor(max_workers=num_workers) as executor:
    list(
        tqdm(
            executor.map(process_group, enumerate(file_groups)),
            total=len(file_groups),
            desc="Processing groups",
        )
    )
