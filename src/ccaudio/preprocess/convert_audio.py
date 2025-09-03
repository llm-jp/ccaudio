from lhotse import MonoCut, MultiCut
from lhotse.cut.data import DataCut


def convert_audio(cut: MonoCut | MultiCut) -> MonoCut:
    if isinstance(cut, MultiCut):
        mono_cut = cut.to_mono(mono_downmix=True)
        assert isinstance(mono_cut, DataCut)
    else:
        mono_cut = cut

    resampled_cut = mono_cut.resample(16000)
    assert isinstance(resampled_cut, MonoCut)

    resampled_cut.supervisions = [resampled_cut.supervisions[0]]

    return resampled_cut
