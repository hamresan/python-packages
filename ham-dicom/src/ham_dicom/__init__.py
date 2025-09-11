"""Lightweight DICOM convenience wrapper.

Usage:
    from ham_dicom import Dicom
    d = Dicom("/path/to/file.dcm")
    print(d.PatientId, d.Year)
"""
from .ham_dicom_handler import Dicom  # re-export
__all__ = ["Dicom"]
from .dicom_converter import DicomConverter  # re-export
__all__ += ["DicomConverter"]