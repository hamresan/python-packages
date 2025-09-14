from __future__ import annotations

import copy
from abc import ABC
from functools import wraps
from typing import Any, Mapping, Optional, Type, Union, Iterable, List, Dict

from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError, IntegrityError

from .query_builder import QueryBuilder

from .serializer import Serializer

class dualmethod:
    """
    Descriptor that lets you define a method once and call it either on the
    instance or on the class. When called on the class, it auto-instantiates.

    Usage:
        class X(AppBaseModel):
            @dualmethod
            def foo(self, ...):
                ...

        X().foo(...)      # works
        X.foo(...)        # also works (creates X() under the hood)
    """
    def __init__(self, func):
        self.func = func
        wraps(func)(self)

    def __get__(self, obj, objtype=None):
        target_type = objtype if obj is None else type(obj)

        @wraps(self.func)
        def wrapper(*args, **kwargs):
            # If accessed via class, create a fresh instance; else use the instance
            self_obj = obj if obj is not None else target_type()
            return self.func(self_obj, *args, **kwargs)

        return wrapper


class AppBaseModel(ABC):
    """
    Thin wrapper that adds convenient CRUD helpers on top of a SQLAlchemy model.
    
    This base class provides a high-level interface for common database operations
    while maintaining the flexibility of SQLAlchemy's ORM.
    
    Subclasses should set `_model` (SQLAlchemy mapped class) and initialize the
    DB session once via `init_db(session)`.

    Attributes:
        _model: SQLAlchemy mapped class (must be set in subclass)
        _db: Database session (initialized via init_db())
        _primary_key: Primary key field name (default: "id")
        _is_allow_insert_manual_id: Allow manual ID setting during insert (default: False)
        _whitelist_fields: List of fields allowed for mass assignment (default: [])
        _guard_fields: List of fields protected from mass assignment (default: [])

    Example:
        class UserModel(AppBaseModel):
            _model = User  # SQLAlchemy mapped class
            
        # Initialize database session
        UserModel.init_db(session)
        
        # Create new user
        user = UserModel.insert({"name": "John", "email": "john@example.com"})
        
        # Find user by ID
        user = UserModel.find(1)
        
        # Update user
        user = UserModel.update({"id": 1, "name": "Jane"})
        
        # Find all users with filters
        users = UserModel.all(filters={"active": True}, limit=10)
    """

    _model: Type[Any] = None
    _db: Session = None

    _primary_key = "id"
    _is_allow_insert_manual_id = False

    # optional field constraints
    _whitelist_fields: List[str] = []
    _guard_fields: List[str] = []

    def __init__(self, entity: Optional[Any] = None, attrs: Optional[dict] = None, db: Optional[Session] = None):
        super().__init__()
        if db is not None:
            self.init_db(db)
        if self._model is None:
            raise RuntimeError("Model is not initialized on subclass (_model).")

        # instantiate or use the provided entity instance
        self._entity = entity if entity is not None else self._model()  # type: ignore[call-arg]

        if attrs:
            self.populate(attrs)

    # ----- configuration helpers -----
    @classmethod
    def init_db(cls, db: Session):
        cls._db = db
        return cls

    @property
    def guard_fields(self) -> List[str]:
        return list(set(self._guard_fields + ["created_at", "creator", "updated_at", "updator"]))

    @property
    def model(self) -> str:
        return self._model

    # ----- model / table names -----
    @property
    def modelname(self) -> str:
        return self._model.__name__

    @property
    def tablename(self) -> str:
        return self._model.__tablename__  # type: ignore[attr-defined]

    # ----- query helpers -----
    @classmethod
    def _ensure_ready(cls) -> None:
        if cls._db is None:
            raise RuntimeError("Database session is not initialized. Call YourModel.init_db(session).")
        if cls._model is None:
            raise RuntimeError("Model is not initialized on subclass (_model).")

    @classmethod
    def first(cls, fields: Iterable[Union[str, Any]] = (), filters: Mapping[str, Any] = None,
              orders: Iterable[Union[str, Any]] = (), include: Iterable[Union[str, Any]] = ()):
        cls._ensure_ready()
        qb = QueryBuilder(cls._db, cls._model).build_query(fields=list(fields or []),
                                                           filters=dict(filters or {}),
                                                           orders=list(orders or []),
                                                           includes=list(include or []))
        entity = qb.one_or_none()
        return cls(entity) if entity else None

    @classmethod
    def find(cls, pk: Any, fields: Iterable[Union[str, Any]] = (), include: Iterable[Union[str, Any]] = ()):
        if pk is None:
            return None
        return cls.first(fields=fields, filters={f"{cls._model.__name__}.{cls._primary_key}": pk}, include=include)
    
    @classmethod
    def find(cls, pk: Any, fields: Iterable[Union[str, Any]] = (), include: Iterable[Union[str, Any]] = ()):
        if pk is None:
            return None
        return cls.first(fields=fields, filters={f"{cls._model.__name__}.{cls._primary_key}": pk}, include=include)    

    @classmethod
    def all(cls, fields: Iterable[Union[str, Any]] = (), filters: Mapping[str, Any] = None,
            orders: Iterable[Union[str, Any]] = (), include: Iterable[Union[str, Any]] = (),
            offset: Optional[int] = None, limit: Optional[int] = None, serialize: bool = False) -> List[Union["AppBaseModel", Dict[str, Any]]]:
        cls._ensure_ready()
        qb = QueryBuilder(cls._db, cls._model).build_query(fields=list(fields or []),
                                                           filters=dict(filters or {}),
                                                           orders=list(orders or []),
                                                           includes=list(include or []),
                                                           offset=offset, limit=limit)
        entities = qb.all()
        
        return ( Serializer.serialize_many(entities, fields=fields, includes=[] ) if serialize else [cls(e) for e in entities] ) if entities else []

    @classmethod
    def exist(cls, field: str, value: Any, exclude_value: Any = None) -> bool:
        if value is None:
            return False
        filters = {field: value}
        if exclude_value is not None:
            filters[f"{cls._primary_key}__ne"] = exclude_value
        obj = cls.first(fields=(cls._primary_key,), filters=filters)
        return obj is not None

    @classmethod
    def count(cls, filters: Mapping[str, Any] = None) -> int:
        cls._ensure_ready()
        qb = QueryBuilder(cls._db, cls._model)
        if filters:
            qb = qb.where(dict(filters))
        return qb.count()

    # ----- persistence hooks -----
    def before_save(self, data: dict) -> dict:
        return data

    def before_insert(self, data: dict) -> dict:
        return data

    def before_update(self, data: dict) -> dict:
        return data

    def after_save(self, old: Optional["AppBaseModel"]) -> bool:
        return True

    def after_insert(self) -> bool:
        return True

    def after_update(self, old: Optional["AppBaseModel"]) -> bool:
        return True

    # ----- persistence -----
    def populate(self, data: Mapping[str, Any]) -> None:
        def sanitize(value: Any) -> Any:
            if isinstance(value, str):
                return value.strip()
            return value

        for field, value in data.items():
            if hasattr(self._entity, field) and (
                (not self._whitelist_fields or field in self._whitelist_fields)
                or ((field not in self.guard_fields) and (field != self._primary_key or self._is_allow_insert_manual_id))
            ):
                setattr(self._entity, field, sanitize(value))

    @dualmethod
    def insert(self, data: Optional[dict] = None) -> Optional["AppBaseModel"]:
        return self._store(data or {}, is_updating=False, is_saving=False)

    @dualmethod
    def update(self, data: Optional[dict] = None) -> Optional["AppBaseModel"]:
        return self._store(data or {}, is_updating=True, is_saving=False)

    @dualmethod
    def save(self, data: Optional[dict] = None) -> Optional["AppBaseModel"]:
        is_updating = getattr(self._entity, self._primary_key, None) is not None or (data and self._primary_key in data)
        return self._store(data or {}, is_updating=is_updating, is_saving=True)

    def _store(self, data: dict, is_updating: bool = False, is_saving: bool = False) -> Optional["AppBaseModel"]:
        type(self)._ensure_ready()

        model_name = type(self).__name__
        pk_name = self._primary_key
        payload = dict(data)

        old_copy = None
        if is_updating:
            pk_val = getattr(self._entity, pk_name, None) or payload.pop(pk_name, None)
            if pk_val is None:
                raise ValueError(f"{model_name} update requires '{pk_name}' in instance or data")
            current = type(self).find(pk_val)
            if not current:
                raise LookupError(f"{model_name} with {pk_name}={pk_val} not found")
            self = current
            old_copy = copy.copy(self)

        try:
            payload = self.before_update(payload) if is_updating else self.before_insert(payload)
            if is_saving:
                payload = self.before_save(payload)

            self.populate(payload)
            type(self)._db.add(self._entity)

            ok = self.after_update(old_copy) if is_updating else self.after_insert()
            if is_saving:
                ok = ok and self.after_save(old_copy)

            if ok:
                type(self)._db.commit()
                type(self)._db.refresh(self._entity)
                return self

            type(self)._db.rollback()
            return None

        except IntegrityError:
            type(self)._db.rollback()
            return None
        except SQLAlchemyError:
            type(self)._db.rollback()
            return None
        except Exception:
            type(self)._db.rollback()
            raise

    # ----- attribute proxying to underlying SQLAlchemy instance -----
    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(f"{type(self).__name__} has no attribute {name!r}")
        if hasattr(self._entity, name):
            return getattr(self._entity, name)
        raise AttributeError(f"{type(self).__name__} has no attribute {name!r}")

    def __setattr__(self, name, value):
        if name.startswith("_") or name in type(self).__dict__:
            object.__setattr__(self, name, value)
            return
        if "_entity" in self.__dict__ and hasattr(self._entity, name):
            setattr(self._entity, name, value)
            return
        object.__setattr__(self, name, value)

    def __delattr__(self, name):
        if name.startswith("_") or name in type(self).__dict__ or hasattr(type(self), name):
            object.__delattr__(self, name)
            return
        if "_entity" in self.__dict__ and hasattr(self._entity, name):
            delattr(self._entity, name)
            return
        raise AttributeError(f"{type(self).__name__} has no attribute {name!r}")

    def __iter__(self):
        for key, value in self._entity.__dict__.items():
            if key != "_sa_instance_state":
                yield (key, value)

    def __str__(self) -> str:
        clean = {k: v for k, v in self._entity.__dict__.items() if k != "_sa_instance_state"}
        return str(clean)
