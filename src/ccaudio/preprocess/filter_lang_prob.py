from lhotse import MonoCut


def filter_lang_prob(cut: MonoCut) -> bool:
    assert cut.custom is not None
    return cut.custom["lang_prob"] >= 0.7
