from lhotse import MonoCut


def filter_lang_prob(cut: MonoCut) -> bool:
    s = cut.supervisions[0]
    assert s.custom is not None
    return s.custom["lang_prob"] >= 0.7
