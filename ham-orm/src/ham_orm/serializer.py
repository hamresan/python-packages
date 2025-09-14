import re
import datetime as dt
from decimal import Decimal
from enum import Enum
from typing import Iterable, Optional, Dict, Any, List

class Serializer:
    """Serialize ORM rows to JSON-friendly dicts with fields, aliases, and includes."""
    _ALIAS_RE = re.compile(r"\s+as\s+", flags=re.IGNORECASE)

    # ---------- public API ----------
    @classmethod
    def serialize_many(cls, rows: Iterable[Any], *, fields: Optional[List[str]] = None,
                        includes: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        return [cls.serialize_row(r, fields=fields, includes=includes) for r in rows]

    @classmethod
    def serialize_row(cls, row: Any, *, fields: Optional[List[str]] = None,
                        includes: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        fields: e.g. ["study_date as StudyDate", "patient.id as PatientId"]
        includes: e.g. ["patient"] -> nested dict under 'patient'
        """
        result: Dict[str, Any] = {}
        rel_field_map: Dict[str, Dict[str, str]] = {}  # {'patient': {'id': 'PatientId', ...}}

        # 1) flatten root & dotted fields, respecting aliases
        for f in (fields or []):
            base, alias = cls._split_alias(f)
            if "." in base:
                rel, leaf = base.split(".", 1)
                val = cls._get_path(row, base)
                # remember which related fields were requested (for nested include rendering)
                rel_field_map.setdefault(rel, {})[leaf] = alias or leaf
                # also place a flattened key
                result[alias or base] = cls._to_primitive(val)
            else:
                val = getattr(row, base)
                result[alias or base] = cls._to_primitive(val)

        # 2) render requested includes as nested dicts/lists
        for rel in (includes or []):
            rel_obj = getattr(row, rel, None)
            if rel_obj is None:
                result[rel] = None
                continue

            req = rel_field_map.get(rel)  # which fields of this relation were requested (if any)
            if isinstance(rel_obj, (list, tuple, set)):
                result[rel] = [cls._serialize_related(item, req) for item in rel_obj]
            else:
                result[rel] = cls._serialize_related(rel_obj, req)

        return result

    # ---------- helpers ----------
    @classmethod
    def _split_alias(cls, s: str) -> tuple[str, Optional[str]]:
        parts = [p.strip() for p in cls._ALIAS_RE.split(s)]
        return (parts[0], parts[1]) if len(parts) == 2 and parts[0] and parts[1] else (s.strip(), None)

    @classmethod
    def _get_path(cls, obj, path: str):
        """
        Walk dotted path. If we hit a collection (InstrumentedList/list/tuple/set),
        we automatically map the *remaining* path over each element.
        Example: Study -> series(list[Series]) -> modality
                 "series.modality" => ["CT", "MR", ...]
        """
        segments = path.split(".")
        return cls._walk(obj, segments)

    @classmethod
    def _walk(cls, obj, segments):
        if not segments:
            return obj

        # If obj is a collection, map the same remaining segments over each item
        if isinstance(obj, (list, tuple, set)):
            return [cls._walk(item, segments) for item in obj]

        # SQLAlchemy collection relation: InstrumentedList
        try:
            from sqlalchemy.orm.collections import InstrumentedList  # local import to avoid hard dep at top
        except Exception:
            InstrumentedList = ()  # fallback if not available

        if InstrumentedList and isinstance(obj, InstrumentedList):
            return [cls._walk(item, segments) for item in obj]

        # Normal attribute hop
        seg = segments[0]
        try:
            next_obj = getattr(obj, seg)
        except AttributeError:
            raise AttributeError(
                f"{type(obj).__name__} has no attribute '{seg}' while resolving '{'.'.join(segments)}'"
            )
        return cls._walk(next_obj, segments[1:])
    
    @classmethod
    def _serialize_related(cls, obj: Any, requested: Optional[Dict[str, str]]) -> Dict[str, Any]:
        if not requested:
            # slim default if no specific related fields requested
            d: Dict[str, Any] = {}
            for pk in ("id", "pk", "uuid"):
                if hasattr(obj, pk):
                    d[pk] = cls._to_primitive(getattr(obj, pk))
                    break
            d["label"] = str(obj)
            return d

        d = {}
        for leaf, out_name in requested.items():
            d[out_name] = cls._to_primitive(getattr(obj, leaf))
        return d

    @classmethod
    def _to_primitive(cls, v):
        # Flatten lists/sets/tuples by converting their elements too
        if isinstance(v, (list, tuple, set)):
            return [cls._to_primitive(x) for x in v]
        # ... keep the rest of your existing _to_primitive logic ...
        import datetime as dt
        from decimal import Decimal
        from enum import Enum

        if v is None or isinstance(v, (str, int, float, bool)):
            return v
        if isinstance(v, dt.datetime):
            return v.isoformat()
        if isinstance(v, (dt.date, dt.time)):
            return v.isoformat()
        if isinstance(v, Decimal):
            return float(v)
        if isinstance(v, Enum):
            return v.value
        return str(v)