from sqlalchemy.orm.collections import collection
from .descriptors import dualmethod
from sqlalchemy.orm import Session

from sqlalchemy.orm import relationship
from sqlalchemy.orm.collections import InstrumentedList

from typing import Any

class ModelCollection(InstrumentedList):
   
    def __init__(self, items=None, db:Session=None):
        # SQLAlchemy passes an iterable here when populating a relationship

        self.bind(db)

        if db and items:
            for item in items:
                item.bind(db)

        super().__init__(items or [])


    @dualmethod
    def bind(self, db: Session) -> "ModelCollection":
        """Bind a Session to this instance (useful for instance-flow APIs)."""
        if db is not None:
            self.__db = db

        return self

    @property
    def _db(self) -> Session:
        return self.__db

    """Generic collection usable for any relationship."""
    @collection.appender
    def append(self, item):
        # hook point: logging, validation, dedup, etc.
        super().append(item)

    @collection.remover
    def remove(self, item):
        super().remove(item)

    # handy utilities
    def first(self):
        return self[0] if self else None

    def to_dicts(self):
        return [getattr(x, "to_dict", lambda: x.__dict__)() for x in self]

    def where(self, pred):
        return ModelCollection([x for x in self if pred(x)])
    

    def by_attr(self, attr, value):
        return [x for x in self if getattr(x, attr, None) == value]

    def sum_attr(self, attr):
        return sum(getattr(x, attr, 0) for x in self)

    def count(self, value: Any = None) -> int:
        return super().count(value) if value is not None else len(self)
    
    def empty(self):
        for item in list(self):  # make a copy so we can iterate safely
            item.delete()

        self.clear()  
        
    def values(self, attr='id'):
        return [getattr(x, attr, None) for x in self]

#apply our custom collection class to all relationships by default
def rel(*args, **kwargs):
    kwargs.setdefault("collection_class", ModelCollection)
    return relationship(*args, **kwargs)
    
