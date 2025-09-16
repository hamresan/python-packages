import os
import re
import io
from datetime import datetime, date, time, timedelta
from pathlib import Path
from typing import Union, Optional, Any
import pydicom
from pydicom.dataset import FileDataset, Dataset
from pydicom.datadict import tag_for_keyword, keyword_for_tag, dictionary_VR, add_private_dict_entry
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

def _parse_dicom_date(date_str: str, use_now: bool = False) -> Optional[date]:
    """Parse DICOM date string (YYYYMMDD) to Python date."""
    if not date_str or len(date_str) < 8:
        return datetime.now().date() if use_now else None
    try:
        return datetime.strptime(date_str[:8], "%Y%m%d").date()
    except ValueError:
        return datetime.now().date() if use_now else None

def _parse_dicom_time(time_str: str) -> Optional[time]:
    """Parse DICOM time string (HHMMSS) to Python time."""
    if not time_str:
        return None
    try:
        # Handle various DICOM time formats
        time_clean = time_str.split('.')[0]  # Remove fractional seconds
        if len(time_clean) >= 6:
            return datetime.strptime(time_clean[:6], "%H%M%S").time()
        elif len(time_clean) >= 4:
            return datetime.strptime(time_clean[:4], "%H%M").time()
        elif len(time_clean) >= 2:
            return datetime.strptime(time_clean[:2], "%H").time()
    except ValueError:
        pass
    return None

def _safe_int(value) -> Optional[int]:
    """Safely convert value to int."""
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None
    
def _safe_float(value) -> Optional[float]:
    """Safely convert value to float."""
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None
       
class _SafeMap(dict):
    """Dict for str.format_map that leaves unknown placeholders as-is."""
    def __missing__(self, key):
        return "{" + key + "}"
        
class Dicom:
    __slots__ = ("dataset",)
    dataset: pydicom.dataset.Dataset
    custom_private_group = 0x0011
    private_creator = "LinkH"
    private_tags = {
        "PatientPhone":  (0x01, "LO"),  # LO = Long String
        "PatientEmail":  (0x02, "UT"),  # UT = Unlimited Text
        "PatientMobile": (0x03, "LO"),
        "StudyStatus":   (0x04, "CS"),  # Code String
    }

    def __init__(
        self,
        dicom: Union[pydicom.dataset.Dataset, _PathLike, dict, str , bytes],
        *,
        force: bool = True,
        stop_before_pixels: bool = False,
        defer_size: Optional[str] = None,
    ) -> None:
        
        ds = None
       
        if isinstance(dicom, pydicom.dataset.Dataset):
            ds = dicom
        elif isinstance(dicom, str):
            ds = pydicom.dcmread(dicom)   
        elif isinstance(dicom, bytes):
            ds = pydicom.dcmread(io.BytesIO(dicom))  
        elif isinstance(dicom, dict):
            ds = Dataset()
            private_block = None
            ds.add_new((self.custom_private_group, 0x0010), 'LO',  self.private_creator)

            for raw_key, value in dicom.items():
                key = raw_key.replace(" ", "")

                if key in self.private_tags:
                    offset, vr = self.private_tags[key]

                    tag = Tag(self.custom_private_group, 0x1000 + offset)

                    add_private_dict_entry(
                        private_creator=self.private_creator,
                        tag=int(tag),
                        VR=vr,
                        description=key,   # shows in print(ds)
                        VM='1'
                    )

                    ds.add_new(tag, vr, value)
                else:
                    tag = tag_for_keyword(key)
                    if tag is not None:
                        vr = dictionary_VR(tag)
                        ds.add_new(tag, vr, value)
                    else:
                        print(f"Unknown DICOM keyword: {key} (skipped)")

        else:
            try:
                ds = pydicom.dcmread(
                    str(dicom),
                    force=False,
                    stop_before_pixels=stop_before_pixels,
                    defer_size=defer_size,
                )
            except pydicom.errors.InvalidDicomError:
                if force:
                    ds = pydicom.dcmread(
                        str(dicom),
                        force=True,
                        stop_before_pixels=stop_before_pixels,
                        defer_size=defer_size,
                    )

            if not 'SOPInstanceUID' in ds:
                raise ValueError("File is not a valid DICOM dataset")
        
        object.__setattr__(self, "dataset", ds)

    
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
                self.ID = getattr(ds, "PatientID", None)
                self.Name = getattr(ds, "PatientName", None)
                self.BirthDate = self._get_birthdate(ds)
                self.Sex = self._get_sex(ds)
                self.Age = getattr(ds, "PatientAge", None)
                self.Weight = _safe_float( getattr(ds, "PatientWeight", None) )
                self.Size = _safe_float ( getattr(ds, "PatientSize", None) )
                self.EthnicGroup = getattr(ds, "EthnicGroup", None)
                self.Occupation = getattr(ds, "Occupation", None)
                self.Comments = getattr(ds, "PatientComments", None)


            def _get_sex(self, ds) -> Optional[str]:
                """
                Convert DICOM PatientSex to human-readable form.
                'M' -> 'Men'
                'F' -> 'Women'
                'O' or anything else -> 'Unknown'
                """
                
                patient_sex = getattr(ds, "PatientSex", None)

                if not patient_sex:
                    return "Unknown" 

                sex = patient_sex.strip().upper()
                if sex == "M":
                    return "Men"
                elif sex == "F":
                    return "Women"
                elif sex == "O":
                    return "Unknown"                
                else:
                    return "Unknown" 

            def _get_birthdate(self, ds) -> Optional[date]:
                """
                Convert DICOM PatientAge (e.g., '032Y', '005M', '010D', '004W') 
                into an approximate birth date.
                Returns a datetime.date or None if invalid.
                """

                birth_date = _parse_dicom_date(getattr(ds, "PatientBirthDate", None))
                age = getattr(ds, "PatientAge", None)

                if birth_date:
                    return birth_date

                if not age:
                    return None

                study_date: date = _parse_dicom_date(getattr(ds, "StudyDate", None), use_now=True)

                try:
                    value = int(age[:3])   # e.g. "032" -> 32
                    unit = age[3].upper()  # e.g. "Y"
                except (ValueError, IndexError):
                    return None

                if unit == "Y":
                    return date(study_date.year - value, study_date.month, study_date.day)
                elif unit == "M":
                    return study_date - timedelta(days=value * 30)  # approx
                elif unit == "W":
                    return study_date - timedelta(weeks=value)
                elif unit == "D":
                    return study_date - timedelta(days=value)

                return None    

            def to_dict(self):
                return {
                    "ID": self.ID,
                    "Name": self.Name,
                    "BirthDate": self.BirthDate,
                    "Sex": self.Sex,
                    "Age": self.Age,
                    "Weight": self.Weight,
                    "Size": self.Size,
                    "EthnicGroup": self.EthnicGroup, 
                    "Occupation": self.Occupation,                                                            
                    "Comments":self.Comments
                }  

            def to_db(self):
                return {
                    "patient_id": self.ID,
                    "patient_name": self.Name,
                    "patient_birth_date": self.BirthDate,
                    "patient_sex": self.Sex,
                    "patient_age": self.Age,
                    "patient_weight": self.Weight,
                    "patient_size": self.Size,
                    "ethnic_group": self.EthnicGroup, 
                    "occupation": self.Occupation,                                                            
                    "patient_comments":self.Comments
                }                 
                       
        return PatientInfo(self.dataset)
    
    # -------- Study --------
    @property
    def Study(self):
        class StudyInfo:
            def __init__(self, ds):
                self.StudyInstanceUID = getattr(ds, 'StudyInstanceUID', None)
                self.StudyID = getattr(ds, 'StudyID', None)
                self.Date = _parse_dicom_date(getattr(ds, 'StudyDate', None))
                self.Time = _parse_dicom_time(getattr(ds, 'StudyTime', None))
                self.Description = getattr(ds, 'StudyDescription', None)
                self.AccessionNumber = getattr(ds, 'AccessionNumber', None)
                self.ReferringPhysicianName = getattr(ds, "ReferringPhysicianName", None)
                self.AttendingPhysicianName = getattr(ds, "AttendingPhysicianName", None)
                self.StudyPriority = getattr(ds, "StudyPriorityID", None)
                self.StudyStatusID = getattr(ds, "StudyStatusID", None)

            def to_dict(self):
                return {
                    "StudyInstanceUID": self.InstanceUID,
                    "StudyID": self.StudyID,
                    "Date": self.Date,
                    "Time": self.Time,
                    "Description": self.Description,
                    "AccessionNumber": self.AccessionNumber,
                    "ReferringPhysicianName": self.ReferringPhysicianName,
                    "AttendingPhysicianName": self.AttendingPhysicianName,
                    "StudyPriority": self.StudyPriority,
                    "StudyStatusID": self.StudyStatusID
                }
            
            def to_db(self):
                return {
                    "study_instance_uid": self.StudyInstanceUID,
                    "study_id": self.StudyID,
                    "study_date": self.Date,
                    "study_time": self.Time,
                    "study_description": self.Description,
                    "accession_number": self.AccessionNumber,
                    "referring_physician_name": self.ReferringPhysicianName,
                    "attending_physician_name": self.AttendingPhysicianName,
                    "study_priority": self.StudyPriority,
                    "study_status_id": self.StudyStatusID
                }
        return StudyInfo(self.dataset)

    # -------- Series --------
    @property
    def Series(self):
        class SeriesInfo:
            def __init__(self, ds):
                self.InstanceUID = getattr(ds, "SeriesInstanceUID", None)
                self.SeriesNumber = _safe_int( getattr(ds, "SeriesNumber", None) )
                self.SeriesDate = _parse_dicom_date( getattr(ds, "SeriesDate", None) )
                self.SeriesTime = _parse_dicom_time( getattr(ds, "SeriesTime", None) )
                self.Description = getattr(ds, "SeriesDescription", None)
                self.Modality = getattr(ds, "Modality", None)
                self.Manufacturer = getattr(ds, "Manufacturer", None)
                self.ManufacturerModelName = getattr(ds, "ManufacturerModelName", None)
                self.StationName = getattr(ds, "StationName", None)
                self.BodyPartExamined = getattr(ds, "BodyPartExamined", None)
                self.ProtocolName = getattr(ds, "ProtocolName", None)
                self.PerformingPhysicianName = getattr(ds, "PerformingPhysicianName", None)
                self.OperatorsName = getattr(ds, "OperatorsName", None)


            def to_dict(self):
                return {
                    "InstanceUID": self.InstanceUID,
                    "SeriesNumber": self.SeriesNumber,
                    "SeriesDate": self.SeriesDate,
                    "SeriesTime": self.SeriesTime,
                    "Description": self.Description,
                    "Modality": self.Modality,
                    "Manufacturer": self.Manufacturer,
                    "ManufacturerModelName": self.ManufacturerModelName,
                    "StationName": self.StationName,
                    "BodyPartExamined": self.BodyPartExamined,
                    "ProtocolName": self.ProtocolName,
                    "PerformingPhysicianName": self.PerformingPhysicianName,
                    "OperatorsName": self.OperatorsName,
                }
            def to_db(self):
                return {
                    "series_instance_uid": self.InstanceUID,
                    "series_number": self.SeriesNumber,
                    "series_date": self.SeriesDate,
                    "series_time": self.SeriesTime,
                    "description": self.Description,
                    "modality": self.Modality,
                    "manufacturer": self.Manufacturer,
                    "manufacturer_model_name": self.ManufacturerModelName,
                    "station_name": self.StationName,
                    "body_part_examined": self.BodyPartExamined,
                    "protocol_name": self.ProtocolName,
                    "performing_physician_name": self.PerformingPhysicianName,
                    "operators_name": self.OperatorsName,
                }
            
        return SeriesInfo(self.dataset)

    # -------- Series --------
    @property
    def Instance(self):
        class InstanceInfo:
            def __init__(self, ds):
                self.SOPInstanceUID = getattr(ds, "SOPInstanceUID", None)
                self.SOPClassUID = getattr(ds, "SOPClassUID", None)
                self.InstanceNumber = _safe_int( getattr(ds, "InstanceNumber", None) )
                self.InstanceCreationDate = _parse_dicom_date( getattr(ds, "InstanceCreationDate", None) )
                self.InstanceCreationTime = _parse_dicom_time( getattr(ds, "InstanceCreationTime", None) )
                self.Rows = _safe_int( getattr(ds, "Rows", None) )
                self.Columns = _safe_int( getattr(ds, "Columns", None) )
                self.BitsAllocated = _safe_int( getattr(ds, "BitsAllocated", None) )
                self.BitsStored = _safe_int( getattr(ds, "BitsStored", None) )
                self.PixelSpacing = getattr(ds, "PixelSpacing", None)
                self.SliceThickness = _safe_float( getattr(ds, "SliceThickness", None) )
                self.SliceLocation = _safe_float( getattr(ds, "SliceLocation", None) )
                self.ImagePositionPatient = getattr(ds, "ImagePositionPatient", None)
                self.FileSize = _safe_int( getattr(ds, "FileSize", None) )
                self.TransferSyntaxUID = getattr(ds, "TransferSyntaxUID", None)
                self.ContentDate = _parse_dicom_date( getattr(ds, "ContentDate", None) )
                self.ContentTime = _parse_dicom_time( getattr(ds, "ContentTime", None) )
                self.AcquisitionDate = _parse_dicom_date( getattr(ds, "AcquisitionDate", None) )
                self.AcquisitionTime = _parse_dicom_time( getattr(ds, "AcquisitionTime", None) )
                self.KVP = _safe_float ( getattr(ds, "KVP", None) )
                self.ExposureTime = _safe_float( getattr(ds, "ExposureTime", None) )
                self.XRayTubeCurrent = _safe_float( getattr(ds, "XRayTubeCurrent", None) )


            def get_positions(self):
                # Extract image position if available
                image_position = self.ImagePositionPatient
                pos_x, pos_y, pos_z = None, None, None
                if image_position:
                    try:
                        positions = [float(x.strip()) for x in str(image_position).split('\\')]
                        if len(positions) >= 3:
                            pos_x, pos_y, pos_z = positions[0], positions[1], positions[2]
                    except (ValueError, AttributeError):
                        pass

                return pos_x, pos_y, pos_z
            

            def get_pixels(self):
                # Extract pixel spacing
                pixel_spacing = self.PixelSpacing
                pixel_x, pixel_y = None, None
                if pixel_spacing:
                    try:
                        spacings = [float(x.strip()) for x in str(pixel_spacing).split('\\')]
                        if len(spacings) >= 2:
                            pixel_x, pixel_y = spacings[0], spacings[1]
                    except (ValueError, AttributeError):
                        pass

                return pixel_x, pixel_y

            def to_dict(self):
                return {
                    "SOPInstanceUID":self.SOPInstanceUID,
                    "SOPClassUID":self.SOPClassUID,
                    "InstanceNumber":self.InstanceNumber,
                    "InstanceCreationDate":self.InstanceCreationDate,
                    "InstanceCreationTime":self.InstanceCreationTime,
                    "Rows":self.Rows,
                    "Columns":self.Columns,
                    "BitsAllocated":self.BitsAllocated,
                    "BitsStored":self.BitsStored,
                    "PixelSpacing":self.PixelSpacing,
                    "SliceThickness":self.SliceThickness,
                    "SliceLocation":self.SliceLocation,
                    "ImagePositionPatient":self.ImagePositionPatient,
                    "SliceThickness":self.SliceThickness,
                    "SliceLocation":self.SliceLocation,
                    "ImageOrientationPatient":self.ImageOrientationPatient,
                    "FileSize":self.FileSize,
                    "TransferSyntaxUID":self.TransferSyntaxUID,
                    "ContentDate":self.ContentDate,
                    "ContentTime":self.ContentTime,
                    "AcquisitionDate":self.AcquisitionDate,
                    "AcquisitionTime":self.AcquisitionTime,
                    "KVP":self.KVP,
                    "ExposureTime":self.ExposureTime,
                    "XRayTubeCurrent":self.XRayTubeCurrent,
                }
            def to_db(self):
                pos_x, pos_y, pos_z = self.get_positions()
                pixel_x, pixel_y = self.get_pixels()

                return {
                    "sop_instance_uid":self.SOPInstanceUID,
                    "sop_class_uid":self.SOPClassUID,
                    "instance_number":self.InstanceNumber,
                    "instance_creation_date":self.InstanceCreationDate,
                    "instance_creation_time":self.InstanceCreationTime,
                    "rows":self.Rows,
                    "columns":self.Columns,
                    "bits_allocated":self.BitsAllocated,
                    "bits_stored":self.BitsStored,
                    "pixel_spacing_x":pixel_x,
                    "pixel_spacing_y":pixel_y,
                    "slice_thickness":self.SliceThickness,
                    "slice_location":self.SliceLocation,
                    "image_position_x":pos_x,
                    "image_position_y":pos_y,
                    "image_position_z":pos_z,
                    "image_orientation":self.ImagePositionPatient,
                    "file_size_bytes":self.FileSize,
                    "transfer_syntax_uid":self.TransferSyntaxUID,
                    "content_date":self.ContentDate,
                    "content_time":self.ContentTime,
                    "acquisition_date":self.AcquisitionDate,
                    "acquisition_time":self.AcquisitionTime,
                    "kvp":self.KVP,
                    "exposure_time":self.ExposureTime,
                    "x_ray_tube_current":self.XRayTubeCurrent,
                }
            
        return InstanceInfo(self.dataset)

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
    def IssuerOfPatientID(self):
        return self.attr('Issuer of Patient ID')

    @property 
    def PrivateCreator(self):
        return self.attr('Private Creator')  

    @property 
    def PrivateID(self):
        return self.attr('Private ID')   

    @property 
    def StudyType(self):
        return 'study'  

    @property 
    def TotalStudyInstances(self):
        return 1  

    @property 
    def TotalStudySeries(self):
        return 1  

    @property 
    def StudyDateSafe(self)->date:
        return _parse_dicom_date(self.attr('StudyDate'), use_now=True)
        
    @property 
    def Year(self) -> str:    
        return self.StudyDateSafe.strftime("%Y")

    @property    
    def Month(self) -> str:    
        return self.StudyDateSafe.strftime("%m")
    
    @property 
    def Day(self) -> str:    
        return self.StudyDateSafe.strftime("%d")
      

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

