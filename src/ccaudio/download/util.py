from pathlib import Path

import requests
from tqdm import tqdm


def download_file(url: str, path: Path, progress_bar: bool = True) -> None:
    with requests.get(url, stream=True) as r:
        r.raise_for_status()

        pbar = None
        if progress_bar:
            pbar = tqdm(
                total=int(r.headers.get("content-length", 0)), unit="B", unit_scale=True
            )

        with open(path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
                if progress_bar:
                    assert pbar is not None
                    pbar.update(len(chunk))
