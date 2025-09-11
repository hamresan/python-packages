# query_builder.py
from __future__ import annotations
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, Type, Union

from sqlalchemy.orm import Session, load_only, selectinload, joinedload
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.sql import ColumnElement
from sqlalchemy import func, asc, desc

LoaderOpt = Any
FilterDict = Dict[str, Any]


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
        if not hasattr(db, "query"):
            raise TypeError("db must be a sync SQLAlchemy Session (has .query)")
        self.db: Session = db
        self.model: Type[Any] = model

        self._joins: List[Tuple[InstrumentedAttribute, bool]] = []   # (attr, isouter)
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
        Load only specific columns on the root model (projection).
        """
        for c in cols:
            if isinstance(c, str):
                attr = self._resolve_attr(self.model, c)
                self._only_cols.append(attr)
            else:
                self._only_cols.append(c)
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
        """
        for it in items:
            if isinstance(it, str):
                direction = desc if it.startswith("-") else asc
                name = it[1:] if it.startswith("-") else it
                col = self._resolve_attr(self.model, name)
                self._order_by.append(direction(col))
            else:
                self._order_by.append(it)
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
        Return a SQLAlchemy Query (legacy ORM style).
        """
        q = self.db.query(self.model)

        # joins
        for attr, isouter in self._joins:
            q = q.join(attr, isouter=isouter)

        # loader options
        if self._includes:
            q = q.options(*self._includes)
        if self._only_cols:
            q = q.options(load_only(*self._only_cols))

        # filters
        if self._filters:
            q = q.filter(*self._filters)

        # order / limit / offset
        if self._order_by:
            q = q.order_by(*self._order_by)
        if self._limit is not None:
            q = q.limit(self._limit)
        if self._offset is not None:
            q = q.offset(self._offset)

        return q
    
    def build_query(self, fields:Union[str, InstrumentedAttribute] = [], filters = {}, orders = [], includes = [], offset = None, limit = None):
        q = self

        if len(fields) > 0:
            q = self.only(*fields)     
               
        if filters:
            q = q.where(filters)

        if len(orders) > 0:
            for order in orders:
                q = q.order_by(order)

        
        if len(includes) > 0:
            for include in includes:
                q = q.include(include)

        if offset is not None:
            q = q.offset(offset)

        if limit is not None:
            q = q.limit(limit)

        return q

    def first(self):
        return self.build().first()

    def one_or_none(self):
        return self.build().one_or_none()

    def all(self):
        return self.build().all()

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
    def _resolve_attr(self, model: Type[Any], name: str) -> InstrumentedAttribute:
        if not hasattr(model, name):
            raise ValueError(f"{model.__name__} has no attribute '{name}'")
        return getattr(model, name)

    def _resolve_attr_path(self, model: Type[Any], path: Union[str, InstrumentedAttribute]) -> InstrumentedAttribute:
        """
        Resolve dotted paths like "patient.studies" relative to root model.
        """
        if not isinstance(path, str):
            return path
        current = model
        parts = path.split(".")
        attr: Optional[InstrumentedAttribute] = None
        for p in parts:
            attr = self._resolve_attr(current, p)
            # walk the mapper to the related class if this is a relationship
            rel = getattr(getattr(current, p), "property", None)
            if rel is not None and hasattr(rel, "mapper"):
                current = rel.mapper.class_
            else:
                current = model  # stay on model for non-relation attrs
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
        """
        preds: List[ColumnElement[bool]] = []
        for key, value in data.items():
            # split field and operator
            if "__" in key:
                raw_field, op = key.split("__", 1)
            else:
                raw_field, op = key, "eq"

            field = self._normalize_field(raw_field)

            col = self._resolve_attr(self.model, field)

            # mapping
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
                # auto wrap value with %...%
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
        return preds
