def test_import():
    from ham_dicom import Dicom
    assert callable(Dicom)
