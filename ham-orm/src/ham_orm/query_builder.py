# query_builder.py
from __future__ import annotations
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, Type, Union
import re
from sqlalchemy import and_, or_

from attr import field

from sqlalchemy.orm import Session, load_only, selectinload, joinedload
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.sql import ColumnElement
from sqlalchemy import func, asc, desc, select
from sqlalchemy.orm import Query

LoaderOpt = Any
FilterDict = Dict[str, Any]

_ALIAS_RE = re.compile(r"\s+as\s+", flags=re.IGNORECASE)

class QueryBuilder:
    """
    Chainable query builder for SQLAlchemy ORM (sync Session).

    Example:
        qb = QueryBuilder(db, Study)\\
                .include("patient")\\
                .where({"id": 8})\\
                .only("id", "study_instance_uid")\\
                .order_by("-id")

        study = qb.first()
    """

    def __init__(self, db: Session, model: Type[Any]):
        # Support both legacy and modern SQLAlchemy sessions
        self.db: Session = db
        self.model: Type[Any] = model
        self._use_legacy_query = hasattr(db, "query")  # Check if legacy query API is available

        self._joins: list[tuple[InstrumentedAttribute, bool]] = []   # (attr, isouter)
        self._includes: List[LoaderOpt] = []                         # loader options
        self._only_cols: List[InstrumentedAttribute] = []            # for load_only
        self._filters: List[ColumnElement[bool]] = []                # where clauses
        self._order_by: List[ColumnElement[Any]] = []
        self._limit: Optional[int] = None
        self._offset: Optional[int] = None

    # ---------- chainable API ----------
    def include(self, *rels_or_opts: Union[str, LoaderOpt]) -> "QueryBuilder":
        """
        Eager-load relationships. Accepts:
          - string names: "patient" -> selectinload(Model.patient)
          - loader options: selectinload(Model.patient), joinedload(...), etc.
        """
        for x in rels_or_opts:
            if isinstance(x, str):
                attr = self._resolve_attr_path(self.model, x)  # supports "patient" or "patient.studies"
                self._includes.append(selectinload(attr))
            else:
                # assume already a loader option
                self._includes.append(x)
        return self

    def only(self, *cols: Union[str, InstrumentedAttribute]) -> "QueryBuilder":
        """
        Load only specific columns on the root model.
        Accepts:
        - "field" or InstrumentedAttribute (root columns)
        - "rel.field" to apply load_only on a relationship
        - Aliases like "field as Alias" (alias ignored at ORM-option time)
        """
        for c in cols:
            if not isinstance(c, str):
                self._only_cols.append(c)
                continue

            base, _alias = self._split_alias(c)

            if "." in base:
                # relationship path like "patient.id"
                rel_path, leaf = base.rsplit(".", 1)
                rel_attr = self._resolve_attr_path(self.model, rel_path)  # Study.patient
                prop = getattr(rel_attr, "property", None)
                if not prop or not hasattr(prop, "mapper"):
                    raise ValueError(f"'{rel_path}' is not a relationship path on {self.model.__name__}")
                target_cls = prop.mapper.class_
                leaf_attr = self._resolve_attr(target_cls, leaf)  # Patient.id
                self._includes.append(selectinload(rel_attr).load_only(leaf_attr))
            else:
                attr = self._resolve_attr(self.model, base)  # Study.study_date
                self._only_cols.append(attr)
        return self
    
    def where(self, filters: Optional[FilterDict] = None,
              *expressions: ColumnElement[bool]) -> "QueryBuilder":
        """
        Add filter criteria. Use either:
          - dict with optional operators via suffixes:
                {"id": 8,
                 "patient_name__ilike": "%ali%",
                 "study_date__between": ("2024-01-01", "2024-12-31"),
                 "patient_sex__in": ["M", "F"],
                 "patient_email__isnull": True}
          - raw SQLAlchemy boolean expressions as *expressions
        """
        if filters:
            self._filters.extend(self._build_predicates(filters))
        if expressions:
            self._filters.extend(expressions)
        return self

    def join(self, *rels: Union[str, InstrumentedAttribute], isouter: bool = False) -> "QueryBuilder":
        """
        Join relationships from the root model. Accepts strings like "patient".
        """
        for r in rels:
            attr = self._resolve_attr_path(self.model, r) if isinstance(r, str) else r
            self._joins.append((attr, isouter))
        return self

    def order_by(self, *items: Union[str, ColumnElement[Any]]) -> "QueryBuilder":
        """
        Order by columns. Strings can be:
        - "field" for ASC
        - "-field" for DESC
        - supports dotted paths like "patient.patient_name" and "series.modality"
            * if the path is a collection relationship (uselist=True), we aggregate:
            ASC -> MIN(leaf), DESC -> MAX(leaf) and GROUP BY the root PK
        """
        for it in items:
            if not isinstance(it, str):
                self._order_by.append(it)
                continue

            direction = desc if it.startswith("-") else asc
            name = it[1:] if it.startswith("-") else it

            if "." not in name:
                col = self._resolve_attr(self.model, name)
                self._order_by.append(direction(col))
                continue

            # dotted path
            rel_path, leaf = name.rsplit(".", 1)
            rel_attr = self._resolve_attr_path(self.model, rel_path)
            prop = getattr(rel_attr, "property", None)
            if not prop or not hasattr(prop, "mapper"):
                raise ValueError(f"'{rel_path}' is not a relationship path on {self.model.__name__}")

            target_cls = prop.mapper.class_
            leaf_col = self._resolve_attr(target_cls, leaf)

            # ensure join (inner is fine; change to outer by passing isouter=True if you prefer)
            if not self._has_join(rel_attr):
                self._joins.append((rel_attr, False))

            if getattr(prop, "uselist", False):
                # collection -> aggregate and group by root PK
                agg = func.max(leaf_col) if it.startswith("-") else func.min(leaf_col)
                self._order_by.append(direction(agg))
                pk = self._root_pk_col()
                if pk not in self._group_by:
                    self._group_by.append(pk)
            else:
                # scalar relation -> simple order by related leaf
                self._order_by.append(direction(leaf_col))

        return self

    def limit(self, n: int) -> "QueryBuilder":
        self._limit = n
        return self

    def offset(self, n: int) -> "QueryBuilder":
        self._offset = n
        return self

    # ---------- builders / runners ----------
    def build(self):
        """
        Return a SQLAlchemy Query or Result, supporting both legacy and modern APIs.
        """
        if self._use_legacy_query:
            # Legacy SQLAlchemy 1.x style
            q = self.db.query(self.model)
        else:
            # Modern SQLAlchemy 2.0+ style
            q = select(self.model)

        # joins
        if self._use_legacy_query:
            for attr, isouter in self._joins:
                q = q.join(attr, isouter=isouter)
        else:
            for attr, isouter in self._joins:
                q = q.join(attr, isouter=isouter)

        # loader options
        if self._includes:
            if self._use_legacy_query:
                q = q.options(*self._includes)
            else:
                q = q.options(*self._includes)
        
        if self._only_cols:
            if self._use_legacy_query:
                q = q.options(load_only(*self._only_cols))
            else:
                q = q.options(load_only(*self._only_cols))

        # filters
        if self._filters:
            if self._use_legacy_query:
                q = q.filter(*self._filters)
            else:
                q = q.where(*self._filters)

        # order / limit / offset
        if self._order_by:
            q = q.order_by(*self._order_by)
        if self._limit is not None:
            q = q.limit(self._limit)
        if self._offset is not None:
            q = q.offset(self._offset)

        return q
    
    def build_query(self, fields: List[Union[str, InstrumentedAttribute]] = None, 
                    filters: FilterDict = None, orders: List[Union[str, ColumnElement[Any]]] = None, 
                    includes: List[Union[str, LoaderOpt]] = None, 
                    offset: Optional[int] = None, limit: Optional[int] = None):
        """
        Convenience method to build a query with all parameters at once.
        
        Args:
            fields: List of field names or attributes to select (for load_only)
            filters: Dictionary of filter conditions
            orders: List of order by clauses
            includes: List of relationships to eager load
            offset: Query offset
            limit: Query limit
            
        Returns:
            Self for method chaining
        """
        q = self

        if fields:
            q = q.only(*fields)     
               
        if filters:
            q = q.where(filters)

        if orders:
            for order in orders:
                q = q.order_by(order)
        
        if includes:
            for include in includes:
                q = q.include(include)

        if offset is not None:
            q = q.offset(offset)

        if limit is not None:
            q = q.limit(limit)

        return q.build()

    def first(self):
        q = self.build()
        if self._use_legacy_query:
            return q.first()
        else:
            # Modern SQLAlchemy 2.0+ style
            return self.db.execute(q).scalar_one_or_none()

    def one_or_none(self):
        q = self.build()
        if self._use_legacy_query:
            return q.one_or_none()
        else:
            # Modern SQLAlchemy 2.0+ style  
            return self.db.execute(q).scalar_one_or_none()

    def all(self):
        q = self.build()
        if self._use_legacy_query:
            return q.all()
        else:
            # Modern SQLAlchemy 2.0+ style
            return self.db.execute(q).scalars().all()
        
    def count(self) -> int:
        """
        Return row count matching current filters/joins.
        """
        if self._use_legacy_query:
            q = self.db.query(func.count()).select_from(self.model)
            if self._filters:
                q = q.filter(*self._filters)
            return q.scalar()
        else:
            q = select(func.count()).select_from(self.model)
            if self._filters:
                q = q.where(*self._filters)
            return self.db.execute(q).scalar_one()       

    def exists(self) -> bool:
        return self.first() is not None

    def to_sql(self) -> str:
        """
        Return a human-readable SQL string (with dialect params).
        """
        q = self.build()
        try:
            # For Query, q.statement compiles to a SQL string
            compiled = q.statement.compile(
                dialect=self.db.bind.dialect, compile_kwargs={"literal_binds": False}
            )
            return str(compiled)
        except Exception:
            # Fallback
            return str(q)
        

    # ---------- helpers ----------
    def _split_alias(self, s: str) -> tuple[str, Optional[str]]:
        """
        Split strings like 'field as Alias' (case-insensitive).
        Returns (base, alias_or_None).
        """
        parts = [p.strip() for p in _ALIAS_RE.split(s)]
        return (parts[0], parts[1]) if len(parts) == 2 else (s.strip(), None)
    
    def _resolve_attr(self, model: Type[Any], name: str) -> InstrumentedAttribute:
        # Strip alias and model prefix before hasattr/getattr
        base, _alias = self._split_alias(name)
        base = self._normalize_field(base)
        if not hasattr(model, base):
            raise ValueError(f"{model.__name__} has no attribute '{base}'")
        return getattr(model, base)

    def _resolve_attr_path(self, model: Type[Any], path: Union[str, InstrumentedAttribute]) -> InstrumentedAttribute:
        if not isinstance(path, str):
            return path
        current = model
        parts = path.split(".")
        attr: Optional[InstrumentedAttribute] = None
        for p in parts:
            attr = self._resolve_attr(current, p)
            rel = getattr(attr, "property", None)
            if rel is not None and hasattr(rel, "mapper"):
                current = rel.mapper.class_
            # else: keep current as-is (do NOT reset to model)
        assert attr is not None
        return attr
     
    def _normalize_field(self, field: str) -> str:
        prefix = f"{self.model.__name__}."
        return field[len(prefix):] if field.startswith(prefix) else field

    def _build_predicates(self, data: FilterDict) -> List[ColumnElement[bool]]:
        """
        Translate dict data into SQLAlchemy boolean expressions.
        Supports suffix operators: __eq, __ne, __lt, __lte, __gt, __gte,
                                __in, __between, __like, __ilike,
                                __startswith, __istartswith, __endswith, __iendswith,
                                __isnull, __notnull
        Default operator is __eq.

        Extended:
        - "__or":  value is a list of dicts and/or SQLAlchemy boolean expressions.
                    Each dict is internally AND-combined, then all are OR-combined.
        - "__and": value is a list of dicts and/or SQLAlchemy boolean expressions.
                    Each dict is internally AND-combined, then all are AND-combined.
        """
        preds: List[ColumnElement[bool]] = []

        # Helper to convert an object (dict or ColumnElement) into a single predicate
        def _to_single_pred(obj) -> ColumnElement[bool]:
            # If user directly supplies a SQLAlchemy boolean expression
            from sqlalchemy.sql.elements import ClauseElement
            if hasattr(obj, "self_group"):  # best-effort duck-typing for SQLA expressions
                return obj  # type: ignore[return-value]

            if isinstance(obj, dict):
                # Build a list of AND'd predicates for this dict
                inner_list = self._build_predicates(obj)
                if not inner_list:
                    # Return a trivially-true AND (or_()/and_() with no args is invalid),
                    # but practically this branch won't be hit if dict has items.
                    return and_(True)  # type: ignore[arg-type]
                return and_(*inner_list)

            raise TypeError(
                "__or/__and expects a list of dicts or SQLAlchemy boolean expressions"
            )

        # First pass: pull out grouping keys to allow mixing with normal keys
        group_ors = data.pop("__or", None) if isinstance(data, dict) else None
        group_ands = data.pop("__and", None) if isinstance(data, dict) else None

        # Handle normal (non-grouping) keys as before (AND behavior)
        if isinstance(data, dict):
            for key, value in data.items():
                # split field and operator
                if "__" in key:
                    raw_field, op = key.split("__", 1)
                else:
                    raw_field, op = key, "eq"

                field = self._normalize_field(raw_field)

                if "." in field:
                    rel_path, leaf = field.rsplit(".", 1)
                    rel_attr = self._resolve_attr_path(self.model, rel_path)  # ensures path
                    prop = getattr(rel_attr, "property", None)
                    if not prop or not hasattr(prop, "mapper"):
                        raise ValueError(f"'{rel_path}' is not a relationship path on {self.model.__name__}")
                    target_cls = prop.mapper.class_
                    col = self._resolve_attr(target_cls, leaf)

                    # ensure we join to filter on related leaf column
                    if not self._has_join(rel_attr):
                        self._joins.append((rel_attr, False))
                else:
                    col = self._resolve_attr(self.model, field)

                # mapping (same as your original)
                if op == "eq":
                    preds.append(col.is_(None) if value is None else (col == value))
                elif op == "ne":
                    preds.append(col.is_not(None) if value is None else (col != value))
                elif op in ("lt", "lte", "gt", "gte"):
                    op_map = {"lt": col < value, "lte": col <= value, "gt": col > value, "gte": col >= value}
                    preds.append(op_map[op])
                elif op == "in":
                    if not isinstance(value, (list, tuple, set)):
                        raise TypeError(f"'{key}' expects a list/tuple/set")
                    preds.append(col.in_(list(value)))
                elif op == "between":
                    if not (isinstance(value, (list, tuple)) and len(value) == 2):
                        raise TypeError(f"'{key}' expects a 2-tuple/list (low, high)")
                    lo, hi = value
                    preds.append(col.between(lo, hi))
                elif op == "like":
                    preds.append(col.like(value))
                elif op == "ilike":
                    preds.append(col.ilike(value))
                elif op in ("contains", "icontains"):
                    pattern = f"%{value}%"
                    preds.append(col.ilike(pattern) if op == "icontains" else col.like(pattern))
                elif op in ("startswith", "istartswith"):
                    pattern = f"{value}%"
                    preds.append(col.ilike(pattern) if op == "istartswith" else col.like(pattern))
                elif op in ("endswith", "iendswith"):
                    pattern = f"%{value}"
                    preds.append(col.ilike(pattern) if op == "iendswith" else col.like(pattern))
                elif op == "isnull":
                    preds.append(col.is_(True if value is True else None))
                elif op == "notnull":
                    preds.append(col.is_not(None))
                else:
                    raise ValueError(f"Unsupported operator '__{op}' for field '{field}'")

        # Handle grouping operators AFTER normal keys so they can wrap sub-dicts
        if group_ors is not None:
            if not isinstance(group_ors, (list, tuple)):
                raise TypeError("'__or' expects a list")
            branches = [_to_single_pred(obj) for obj in group_ors]
            preds.append(or_(*branches))

        if group_ands is not None:
            if not isinstance(group_ands, (list, tuple)):
                raise TypeError("'__and' expects a list")
            branches = [_to_single_pred(obj) for obj in group_ands]
            preds.append(and_(*branches))

        return preds

    def _has_join(self, rel_attr, isouter: bool | None = None) -> bool:
            for a, outer in self._joins:
                if a is rel_attr and (isouter is None or outer is isouter):
                    return True
            return False