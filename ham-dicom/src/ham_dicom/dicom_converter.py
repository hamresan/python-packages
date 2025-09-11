import os
import re
from pathlib import Path
from typing import Union, Optional, Any
import pydicom
from pydicom.dataset import FileDataset
from pydicom.datadict import tag_for_keyword, keyword_for_tag
from pydicom.tag import Tag

_PathLike = Union[str, Path]

def _normalize_id(s: str) -> str:
    if not s:
        return ""
    # حذف فاصله‌های اضافی و تبدیل به خط‌تیره برای پایداری
    s = s.strip()
    s = re.sub(r"\s+", "-", s)
    # حذف کاراکترهای مشکل‌زا
    s = re.sub(r"[^A-Za-z0-9._\-]", "", s)
    return s


class _SafeMap(dict):
    """Dict for str.format_map that leaves unknown placeholders as-is."""
    def __missing__(self, key):
        return "{" + key + "}"
        
class DicomConverter:
    __slots__ = ("dataset",)
    defaults = {
        "IssuerOfPatientID": "",                   # known keyword
        Tag(0x00090010): ("LO", ""),               # PrivateCreator (0009,0010) example
        Tag(0x00091011): ("LO", ""),               # PrivateID    (0009,1011) example
        "StudyType": "",                           # custom (you’ll need to decide tag if you want real DICOM)
        Tag(0x00201208): ("IS", 1),                # NumberOfStudyRelatedInstances
        Tag(0x00201206): ("IS", 1),                # NumberOfStudyRelatedSeries
    }    
    dataset: pydicom.dataset.Dataset

    def __init__(
        self,
        dicom: Union[pydicom.dataset.Dataset, _PathLike],
        *,
        force: bool = True,
        stop_before_pixels: bool = False,
        defer_size: Optional[str] = None,
    ) -> None:
        
        dataset = None
        
        if isinstance(dicom, pydicom.dataset.Dataset):
            dataset = dicom
        else:
            try:
                dataset = pydicom.dcmread(
                    str(dicom),
                    force=False,
                    stop_before_pixels=stop_before_pixels,
                    defer_size=defer_size,
                )
            except pydicom.errors.InvalidDicomError:
                if force:
                    dataset = pydicom.dcmread(
                        str(dicom),
                        force=True,
                        stop_before_pixels=stop_before_pixels,
                        defer_size=defer_size,
                    )

        if not 'SOPInstanceUID' in dataset:
            raise ValueError("File is not a valid DICOM dataset")

        for key, val in self.defaults.items():
            if isinstance(key, str):  # keyword
                if not key in dataset:
                    setattr(dataset, key, val)
            else:  # tag
                if key not in dataset:
                    vr, value = val
                    dataset.add_new(key, vr, value)
        
        object.__setattr__(self, "dataset", dataset)

    
    def ensure_file_meta(self):
        from pydicom.uid import ExplicitVRLittleEndian
        from pydicom.dataset import Dataset

        if not self.hasattr("file_meta") or not hasattr(self.file_meta, "TransferSyntaxUID"):
                self.file_meta = Dataset()
                self.file_meta.TransferSyntaxUID = ExplicitVRLittleEndian
                self.file_meta.MediaStorageSOPClassUID = self.SOPClassUID
                self.file_meta.MediaStorageSOPInstanceUID = self.SOPInstanceUID

    # -------- Institution --------
    @property
    def Institution(self):
        class InstitutionInfo:
            def __init__(self, ds):
                self.Name = getattr(ds, "InstitutionName", None)
                self.Address = getattr(ds, "InstitutionAddress", None)
                self.StationName = getattr(ds, "StationName", None)

                issuer = getattr(ds, "IssuerOfPatientID", None)
                if issuer:
                    self.ID = issuer
                else:
                    # Generate a unique, standard-compliant ID based on name and address
                    self.ID = None#generate_uid(prefix=f"1.2.826.0.1.3680043.10.543.{hash_part}")
                    nid = "-".join(
                        x for x in [
                            _normalize_id(str(self.Name)) if self.Name else "",
                            _normalize_id(str(self.Address)) if self.Address else ""
                        ] if x
                    )
                    self.ID = nid or None


            def to_dict(self):
                return {
                    "Name": self.Name,
                    "ID": self.ID,
                    "Address": self.Address,
                    "StationName": self.StationName
                }
        return InstitutionInfo(self.dataset)

    # -------- Patient --------
    @property
    def Patient(self):
        class PatientInfo:
            def __init__(self, ds):
                self.Name = ds.PatientName
                self.ID = ds.PatientID
                self.BirthDate = ds.PatientBirthDate
                self.Sex = ds.PatientSex
                self.Age = ds.PatientAge
                self.Comments = ds.PatientComments
 
            def to_dict(self):
                return {
                    "Name": self.Name,
                    "ID": self.ID,
                    "BirthDate": self.BirthDate,
                    "Sex": self.Sex,
                    "Age": self.Age,
                    "Comments":self.Comments
                }                
        return PatientInfo(self.dataset)
    
    # -------- Study --------
    @property
    def Study(self):
        class StudyInfo:
            def __init__(self, ds):
                self.InstanceUID = ds.StudyInstanceUID
                self.StudyID = ds.StudyID
                self.Date = ds.StudyDate
                self.Time = ds.StudyTime
                self.Description = ds.StudyDescription
                self.AccessionNumber = ds.AccessionNumber

            def to_dict(self):
                return {
                    "InstanceUID": self.InstanceUID,
                    "StudyID": self.StudyID,
                    "Date": self.Date,
                    "Time": self.Time,
                    "Description": self.Description,
                    "AccessionNumber": self.AccessionNumber
                }
        return StudyInfo(self.dataset)

    # -------- Series --------
    @property
    def Series(self):
        class SeriesInfo:
            def __init__(self, ds):
                self.InstanceUID = ds.SeriesInstanceUID
                self.SeriesNumber = ds.SeriesNumber
                self.Description = ds.SeriesDescription
                self.Modality = ds.Modality
                self.BodyPartExamined = ds.BodyPartExamined
                self.Laterality = ds.Laterality
                self.ImageLaterality = ds.ImageLaterality
                self.ProtocolName = ds.ProtocolName

            def to_dict(self):
                return {
                    "InstanceUID": self.InstanceUID,
                    "SeriesNumber": self.SeriesNumber,
                    "Description": self.Description,
                    "Modality": self.Modality,
                    "BodyPartExamined": self.BodyPartExamined,
                    "Laterality": self.Laterality,
                    "ImageLaterality": self.ImageLaterality,
                    "ProtocolName": self.ProtocolName
                }
        return SeriesInfo(self.dataset)

    # -------- Equipment --------
    @property
    def Equipment(self):
        class EquipmentInfo:
            def __init__(self, ds):
                self.Manufacturer = ds.Manufacturer
                self.ManufacturerModelName = ds.ManufacturerModelName
                self.DeviceSerialNumber = ds.DeviceSerialNumber
                self.SoftwareVersions = ds.SoftwareVersions
                self.DetectorType = ds.DetectorType
                self.Grid = ds.Grid
                self.DistanceSourceToDetector = ds.DistanceSourceToDetector
                self.KVP = ds.KVP
                self.XRayTubeCurrent = ds.XRayTubeCurrent
                self.Exposure = ds.Exposure

            def to_dict(self):
                return {
                    "Manufacturer": self.Manufacturer,
                    "ManufacturerModelName": self.ManufacturerModelName,
                    "DeviceSerialNumber": self.DeviceSerialNumber,
                    "SoftwareVersions": self.SoftwareVersions,
                    "DetectorType": self.DetectorType,
                    "Grid": self.Grid,
                    "DistanceSourceToDetector": self.DistanceSourceToDetector,
                    "KVP": self.KVP,
                    "XRayTubeCurrent": self.XRayTubeCurrent,
                    "Exposure": self.Exposure
                }
        return EquipmentInfo(self.dataset)

    # -------- Image --------
    @property
    def Image(self):
        class ImageInfo:
            def __init__(self, ds):
                self.SOPClassUID = ds.SOPClassUID
                self.SOPInstanceUID = ds.SOPInstanceUID
                self.Rows = ds.Rows
                self.Columns = ds.Columns
                self.BitsAllocated = ds.BitsAllocated
                self.BitsStored = ds.BitsStored
                self.HighBit = ds.HighBit
                self.PixelRepresentation = ds.PixelRepresentation
                self.PhotometricInterpretation = ds.PhotometricInterpretation
                self.PixelSpacing = ds.PixelSpacing
                self.ImagerPixelSpacing = ds.ImagerPixelSpacing
                self.WindowCenter = ds.WindowCenter
                self.WindowWidth = ds.WindowWidth
                self.RescaleIntercept = ds.RescaleIntercept
                self.RescaleSlope = ds.RescaleSlope
                self.LossyImageCompression = ds.LossyImageCompression

            def to_dict(self):
                return {
                    "SOPClassUID": self.SOPClassUID,
                    "SOPInstanceUID": self.SOPInstanceUID,
                    "Rows": self.Rows,
                    "Columns": self.Columns,
                    "BitsAllocated": self.BitsAllocated,
                    "BitsStored": self.BitsStored,
                    "HighBit": self.HighBit,
                    "PixelRepresentation": self.PixelRepresentation,
                    "PhotometricInterpretation": self.PhotometricInterpretation,
                    "PixelSpacing": self.PixelSpacing,
                    "ImagerPixelSpacing": self.ImagerPixelSpacing,
                    "WindowCenter": self.WindowCenter,
                    "WindowWidth": self.WindowWidth,
                    "RescaleIntercept": self.RescaleIntercept,
                    "RescaleSlope": self.RescaleSlope,
                    "LossyImageCompression": self.LossyImageCompression
                }
        return ImageInfo(self.dataset)   

    def get_all_info_properties(self):
        """
        Returns all custom info properties (Institution, Patient, Study, etc.) as a dictionary
        """
        properties = {}
        
        # Get all properties of the class
        for attr_name in dir(self.__class__):
            attr = getattr(self.__class__, attr_name)
            
            # Check if it's a property and not a built-in attribute
            if isinstance(attr, property) and not attr_name.startswith('_'):
                # Get the property value
                try:
                    value = getattr(self, attr_name)
                    # Check if the property has a to_dict method (our custom info classes)
                    if hasattr(value, 'to_dict'):
                        properties[attr_name] = value.to_dict()
                    else:
                        properties[attr_name] = value
                except AttributeError:
                    # Skip properties that might raise AttributeError due to missing DICOM tags
                    continue
        
        return properties
    
    def get_all_info_objects(self):
        """
        Returns all custom info objects (Institution, Patient, Study, etc.) as a dictionary
        """
        objects = {}
        
        for attr_name in dir(self.__class__):
            attr = getattr(self.__class__, attr_name)
            
            if isinstance(attr, property) and not attr_name.startswith('_'):
                try:
                    value = getattr(self, attr_name)
                    if hasattr(value, 'to_dict'):
                        objects[attr_name] = value
                except AttributeError:
                    continue
        
        return objects        

    @property 
    def Id(self):
        return self.attr('SOP Instance UID')

    @property 
    def PatientId(self):
        return self.attr('Patient ID')
    
       
        
    @property 
    def Year(self):    
        date = self.attr('StudyDate')
        return date[:4]
    @property 
    
    def Month(self):    
        date = self.attr('StudyDate')
        return date[4:6]
    
    @property 
    def Day(self):    
        date = self.attr('StudyDate')
        return date[4:6]        

    def asdict(self, include_dataset: bool = True, include_wrapper: bool = True) -> dict:
        """
        Build a dict of available fields for formatting or inspection.
        Wrapper props + dataset keywords (present only), excluding heavy/binary data.
        """
        result = {}
        exclude = {"PixelData"}  # <-- skip these keywords
    
        if include_wrapper:
            for name in dir(self):
                if name.startswith("_"):
                    continue
                try:
                    val = getattr(self, name)
                except Exception:
                    continue
                if callable(val) or name == "dataset":
                    continue
                result[name] = val
    
        if include_dataset:
            for kw in self.dataset.dir():
                if kw in exclude:
                    continue
                try:
                    result[kw] = getattr(self.dataset, kw)
                except Exception:
                    pass
                # also add spaced alias
                spaced = "".join(ch if ch.islower() else f" {ch}" for ch in kw).strip().replace(" U I D", " UID")
                if spaced != kw:
                    result[spaced] = result.get(kw)
    
        return result  
    
    def format(self, fmt: str, **extra) -> str:
        """
        Format a string using wrapper props and dataset attrs.

        Examples:
            self.format("Patient: {PatientName}, Date: {Year}-{Month}-{Day}")
            self.format("SMS: {PatientId} is ready for exam at {StudyDate}")
        """
        # Build a lookup mapping with priorities: extra > wrapper > dataset
        data = self.asdict()

        # extras take precedence
        data.update(extra)
        return f"{fmt}".format_map(_SafeMap(data))

    def save(self, filepath, write_like_original=False)->bool:
        self.dataset.save_as(filepath, write_like_original=write_like_original)
        return os.path.exists(filepath)
    
    def hasattr(self, name: str) -> bool:
        return hasattr(self.dataset, name)    
        
    # ---------- attribute delegation ----------
    def __hasattr__(self, name: str) -> Any:
        # called only if normal attribute lookup fails
        return hasattr(self.dataset, name)
    
    def __getattr__(self, name: str) -> Any:
        # called only if normal attribute lookup fails      
        return getattr(self.dataset, name) if self.hasattr(name) else None

    def __setattr__(self, name: str, value: Any) -> None:
        if name in self.__slots__:
            object.__setattr__(self, name, value)
        else:
            setattr(self.dataset, name, value)

    def __delattr__(self, name: str) -> None:
        if name in self.__slots__:
            object.__delattr__(self, name)
        else:
            delattr(self.dataset, name)

    # ---------- mapping / container delegation ----------
    def __getitem__(self, key: Any) -> Any:
        return self.dataset[key]

    def __setitem__(self, key: Any, value: Any) -> None:
        self.dataset[key] = value

    def __delitem__(self, key: Any) -> None:
        del self.dataset[key]

    def __contains__(self, item: Any) -> bool:
        return item in self.dataset

    def __iter__(self):
        return iter(self.dataset)

    def __len__(self) -> int:
        return len(self.dataset)

    # ---------- conversion / display ----------
    def __str__(self) -> str:
        return str(self.dataset)

    def __array__(self, dtype: Optional[Any] = None):
        """
        NumPy protocol. Prefer the dataset's own __array__ if available;
        otherwise try pixel_array (common for DICOM images).
        """
        arr_meth = getattr(self.dataset, "__array__", None)
        if callable(arr_meth):
            return arr_meth(dtype)

        if hasattr(self.dataset, "pixel_array"):
            arr = self.dataset.pixel_array
            if dtype is not None:
                return arr.astype(dtype, copy=False)
            return arr

        raise TypeError("Underlying dataset does not provide an array interface.")

    def attr(self, name: str, default: Any = None) -> Any:
        """
        Fetch an attribute from the underlying dataset.

        Args:
            name: The attribute name to get (case-sensitive like pydicom attrs).
            default: Value to return if attribute is missing.

        Returns:
            The attribute value if it exists, otherwise `default`.
        """
        keyword = name.replace(" ", "") 
        return getattr(self.dataset, keyword, default)


