from __future__ import annotations

from abc import ABC
from typing import Any, Dict, Iterable, List, Mapping, Optional, Union
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import Session
from sqlalchemy.sql import ColumnElement

from .descriptors import dualmethod
from .tx import tx_wrap, HookFailed
from .query_builder import QueryBuilder
from .db import Base
from .collection import ModelCollection

from .serializer import Serializer

class AppBaseModel:
    __abstract__ = True

    """
    Session-aware CRUD mixin for SQLAlchemy models.
    Assumes concrete models inherit both (AppBaseModel, Base).
    """

    _whitelist_fields: List[str] = []
    _guard_fields: List[str] = []

    #def __init__(self, *args, db=None, **kwargs):

    #    self.bind( db )
        # directly call SQLAlchemy’s built-in constructor
    #    super().__init__(**kwargs)

    # -------------------- Session wiring --------------------
    @dualmethod
    def bind(self, db: Session, autocommit=False) -> "AppBaseModel":
        """Bind a Session to this instance (useful for instance-flow APIs)."""
        if db is not None:
            self.__db = db

        self.autocommit = autocommit

        return self

    @property
    def _db(self) -> Session:
        return self.__db
    
     # -------------------- Introspection helpers --------------------
    @property
    def _primary_key(self) -> str:
        # prefer 'id' if present
        if "id" in self.__table__.columns:
            return "id"
        for col in self.__table__.columns:
            if col.primary_key:
                return col.name
        raise ValueError(f"No primary key defined for {self.__class__.__name__}")
    
    @property
    def _primary_key_value(self) -> Any:
        pk = self._primary_key
        return getattr(self, pk, None)  

    @property
    def unique_columns(self) -> List[str]:
        return [col.name for col in self.__table__.columns if col.unique or col.primary_key]
    
    @property
    def has_soft_delete(self) -> bool:
        return "deleted_at" in self.__table__.columns
    
    @property
    def is_deleted(self) -> bool:
        return self.deleted_at is not None    

    @dualmethod
    def _ensure_ready(self):
        if self._db is None:
            raise RuntimeError(f"Database session is not initialized. create db.")
        
    @dualmethod
    def is_autocommit_enabled(self) -> bool:
        """
        If you’re using the tx-via-with marker (db.info['tx_via_with']),
        this returns True when a `with db.begin():` opened the tx.
        Override if you prefer a different policy.
        """
        self._ensure_ready()
        return self.autocommit or bool(getattr(self._db, "info", {}).get("tx_via_with", False))

    @property
    def in_transaction(self) -> bool:
        self._ensure_ready()
        db = self._db
        return bool(db.in_transaction() or db.in_nested_transaction())

    # -------------------- Query helpers (optional, QueryBuilder-based) --------------------
    @dualmethod
    def first(self, fields: Iterable[Union[str, Any]] = (), filters: Mapping[str, Any] = None,
              orders: Union[str, Iterable[Union[str, Any]]] = None, 
              include: Iterable[Union[str, Any]] = ()):
        
        self._ensure_ready()

        if isinstance(orders, (str, ColumnElement)):  
            orders = orders.split(",") if isinstance(orders, str) else [orders]           

        if QueryBuilder is None:
            raise RuntimeError("QueryBuilder not available; ensure ham_orm.query_builder is importable.")
        
        qb = QueryBuilder(self._db, self.__class__).build_query(
            fields=list(fields or []),
            filters=dict(filters or {}),
            orders=list(orders or []),
            includes=list(include or []),
        )
        entity = qb.first()
        return entity.bind(self._db) if entity else None

    @dualmethod
    def find(self, pk: Any, fields: Iterable[Union[str, Any]] = (), include: Iterable[Union[str, Any]] = ()):
        if pk is None:
            return None
        
        self._ensure_ready()

        return self.first(fields=fields,
                          filters={f"{self.__class__.__name__}.{self._primary_key}": pk},
                          include=include)
    

    @dualmethod
    def find_by(self, field:str, value: Any, fields: Iterable[Union[str, Any]] = (), include: Iterable[Union[str, Any]] = ()):
        
        self._ensure_ready()

        return self.first(fields=fields,
                          filters={f"{self.__class__.__name__}.{field}": value},
                          include=include)
    
    @dualmethod
    def all(self, fields: Iterable[Union[str, Any]] = (), filters: Mapping[str, Any] = None,
            orders: Union[str, Iterable[Union[str, Any]]] = None, 
            include: Iterable[Union[str, Any]] = (),
            offset: Optional[int] = None, limit: Optional[int] = None, serialize: bool = False) -> ModelCollection:
        
        self._ensure_ready()

        if isinstance(orders, (str, ColumnElement)):  
            orders = orders.split(",") if isinstance(orders, str) else [orders]   

        if QueryBuilder is None:
            raise RuntimeError("QueryBuilder not available; ensure ham_orm.query_builder is importable.")
        qb = QueryBuilder(self._db, self.__class__).build_query(
            fields=list(fields or []),
            filters=dict(filters or {}),
            orders=list(orders or []),
            includes=list(include or []),
            offset=offset,
            limit=limit,
        )
        items  = qb.all()

        return Serializer.serialize_many(items, fields=fields, includes=[] ) if serialize else ModelCollection(items, self._db)
    
       
    @dualmethod
    def paginate(self, fields: Iterable[Union[str, Any]] = (), filters: Mapping[str, Any] = None,
            orders: Union[str, Iterable[Union[str, Any]]] = None,            
            include: Iterable[Union[str, Any]] = (),
            offset: Optional[int] = None, limit: Optional[int] = None, serialize: bool = False) -> List["AppBaseModel"]:    
    
        self._ensure_ready()

        return self.all(fields=fields, filters=filters, orders=orders, include=include, offset=offset, limit=limit, serialize=serialize), self.count(filters=filters)
    
    @dualmethod
    def start_transaction(self) -> Any:
        self._ensure_ready()
        return self._db.begin()

    # -------------------- Data population --------------------
    @dualmethod
    def populate(self, data: dict) -> "AppBaseModel":
        self._ensure_ready()

        if data is None:
            return self

        mapper = inspect(self.__class__)
        model_cols = {c.key for c in mapper.column_attrs}

        guard_fields = list(self._guard_fields or [])
        pk = self._primary_key
        if pk:
            guard_fields.append(pk)

        allowed = model_cols
        if self._whitelist_fields:
            allowed = allowed & set(self._whitelist_fields)
        if guard_fields:
            allowed = allowed - set(guard_fields)

        for k, v in data.items():
            if k in allowed:
                setattr(self, k, v)

        return self
    
    @dualmethod
    def cleanup(self) -> "AppBaseModel":
        self._ensure_ready()

        for col in ['deleted_at', 'deleted_by', 'deletion_reason']:
            if hasattr(self, col):
                delattr(self, col)
        return self
    
    # -------------------- Existence / counts --------------------
    @dualmethod
    def exists(self, value: Any, field: str = None, exclude_value: Any = None) -> bool:
        if value is None:
            return False
        
        self._ensure_ready()
        
        field = field or self._primary_key
        filters = {field: value}
        if exclude_value is not None:
            filters[f"{self._primary_key}__ne"] = exclude_value

        return self.first(fields=(self._primary_key,), filters=filters) is not None

    @dualmethod
    def count(self, filters: Mapping[str, Any] = None) -> int:
        self._ensure_ready()
        if QueryBuilder is None:
            raise RuntimeError("QueryBuilder not available; ensure ham_orm.query_builder is importable.")
        qb = QueryBuilder(self._db, self.__class__)
        if filters:
            qb = qb.where(dict(filters))
        return qb.count()

    # -------------------- Hooks (override in models/services as needed) --------------------
    def before_create(self) -> bool: return True
    def after_create(self) -> bool: return True
    def before_update(self) -> bool: return True
    def after_update(self, before) -> bool: return True
    def before_save(self) -> bool: return True
    def after_save(self, before:None) -> bool: return True
    def before_delete(self) -> bool: return True
    def after_delete(self, before) -> bool: return True

    def before_soft_delete(self) -> bool: return True
    def after_soft_delete(self) -> bool: return True    

    def before_restore(self) -> bool: return True
    def after_restore(self) -> bool: return True        

    # -------------------- CRUD (tx-managed by decorator) --------------------
    @dualmethod
    @tx_wrap(refresh_on_commit=True, return_self_on_success=False)
    def create(self, data: Optional[dict] = None) -> Optional["AppBaseModel"]:
        self._ensure_ready()
        obj = self.populate(data) if data else self
        
        if not obj.before_create() or not obj.before_save():
            return None
        
        obj._db.add(obj.cleanup())
        obj._db.flush()
        return obj if obj.after_create() and obj.after_save(None) else None

    @dualmethod
    @tx_wrap(refresh_on_commit=True, return_self_on_success=False)
    def update(self, data: Optional[dict] = None) -> Optional["AppBaseModel"]:
        self._ensure_ready()
        has_data = data is not None and bool(data)

        pk_name = self._primary_key
        pk_value = getattr(self, pk_name, None)
        if pk_value is None:
            raise ValueError("Cannot update an object without a primary key value")
        
        before_data = dict( self.find(pk_value) )

        obj = self.populate(data) if has_data else self

        if not obj.before_update() or not obj.before_save():
            return None
                
        obj = obj.cleanup()

        if has_data:
            self._db.add(obj.cleanup())

        obj._db.flush()

        before = type(self)(**before_data).bind(self._db)

        return obj if obj.after_update(before) and obj.after_save(before) else None
    
    @dualmethod
    def find_by_unique_columns(self, data: Dict) -> type["AppBaseModel"]:   
        self._ensure_ready()
        for col in self.unique_columns:
            if col in data:
                obj = self.find_by(col, data[col])
                if obj:
                    return obj
        return None
    
    @dualmethod
    def upsert(self, data: Dict) -> type["AppBaseModel"]:
        self._ensure_ready()
        obj = self.find_by_unique_columns(data)
        
        return obj.update(data) if obj else self.create(data)

    @dualmethod
    @tx_wrap(refresh_on_commit=True, return_self_on_success=False)
    def save(self, data: Optional[dict] = None) -> Optional["AppBaseModel"]:
        self._ensure_ready()
        has_data = data is not None and bool(data)

        pk_value = getattr(self, self._primary_key, None)
        is_update = pk_value is not None

        obj = self.populate(data) if has_data else self

        if is_update:
            _before = self.find(pk_value)
            if _before is None:
                raise ValueError(f"Cannot update non-existing {self.__class__.__name__} with {self._primary_key}={pk_value}")
            
            before_data = dict( _before )


        if not self.before_save():
            return None
        
        if is_update and not self.before_update():
            return None
        if not is_update and not self.before_create():
            return None

        obj = obj.cleanup()
        
        if has_data or not is_update:
            self._db.add(obj)
        
        self._db.flush()

        ok = obj.after_save(self)

        ok = ok and (obj.after_update(type(self)(**before_data).bind(self._db)) if is_update else obj.after_create())

        return obj if ok else None

    @dualmethod
    @tx_wrap(refresh_on_commit=True, return_self_on_success=False)
    def delete(self, value: Any = None, field: str = None, permanent:bool=False) -> bool:
        self._ensure_ready()

        if self.has_soft_delete and not permanent:
            return self.soft_delete() is not None

        field = field or self._primary_key

        obj = self.first(filters={field: value}) if value is not None else self

        if obj is None or getattr(obj, self._primary_key, None) is None:
            raise ValueError(f"Cannot delete a non-existing {self.__class__.__name__}")
        
        #before_data = dict(obj)
        
        if not obj.before_delete():
            return None
        
        obj._db.delete(obj)
        obj._db.flush()

        #before = type(self)(**before_data).bind(self._db)

        return self.after_delete(obj)
    
    @dualmethod
    @tx_wrap(refresh_on_commit=True, return_self_on_success=False)    
    def soft_delete(self, by: Optional[int] = None, reason: Optional[str] = None) -> None:
        from datetime import datetime, timezone
        if not self.has_soft_delete():
            raise RuntimeError(f"Model {self.__class__.__name__} does not support soft deletion (no deleted_at column)")
        
        if self.deleted_at is not None:
            return  # already deleted
        
        if not self.before_soft_delete():
            return
        
        self.deleted_at = datetime.now(timezone.utc)
        if by is not None and hasattr(self, "deleted_by"):
            self.deleted_by = by
        if reason is not None and hasattr(self, "deletion_reason"):
            self.deletion_reason = reason

        self._db.flush()

        return self.after_soft_delete() 
    
    @dualmethod
    @tx_wrap(refresh_on_commit=True, return_self_on_success=False)    
    def restore(self) -> None:
        from datetime import datetime, timezone
        if not self.has_soft_delete():
            raise RuntimeError(f"Model {self.__class__.__name__} does not support restore (no deleted_at column)")
        
        if self.deleted_at is None:
            return  # already restored
        
        if not self.before_restore():
            return
        
        self.deleted_at = None
        if hasattr(self, "deleted_by"):
            self.deleted_by = None

        if hasattr(self, "deletion_reason"):
            self.deletion_reason = None

        self._db.flush()

        return self.after_restore() 
    

    @dualmethod
    @tx_wrap(refresh_on_commit=True, return_self_on_success=False)
    def get_or_create(self, data: Optional[dict], filters: Mapping[str, Any] = None) -> Optional["AppBaseModel"]:
        self._ensure_ready()

        if filters is None:
            filters = data
       
        entity = self.first(filters=filters)

        if entity is None:
            entity = self.create(data=data)

        return entity
        
    @dualmethod
    @tx_wrap(refresh_on_commit=True, return_self_on_success=False)
    def create_or_update(self, data: Optional[dict], filters: Mapping[str, Any]=None) -> Optional["AppBaseModel"]:
        self._ensure_ready()
        
        if filters is None:
            filters = data

        entity = self.first(filters=filters)

        if entity is None:
            entity = entity = self.create(data=data)
        else:
            entity = entity.update(data=data)

        return entity

    def __getattr__(self, name):
        if name.startswith("find_by_"):
            field = name[len("find_by_"):]  # e.g. "uid" from "find_by_uid"

            def finder(value, fields: Iterable[Union[str, Any]] = (), include: Iterable[Union[str, Any]] = ()):
                # Here you implement your logic, like querying the DB
                # Example return (replace with ORM query)
                return self.find_by(field, value, fields=fields, include=include)

            return finder
        raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{name}'")

    def __iter__(self):
        for c in self.__table__.columns:
            yield (c.name, getattr(self, c.name))
