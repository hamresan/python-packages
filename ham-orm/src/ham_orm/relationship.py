# rel.py
from sqlalchemy.orm import relationship as rel
from .collection import ModelCollection

def relationship(*args, **kwargs):
    # only applies to collection-side relationships (uselist=True)
    kwargs.setdefault("collection_class", ModelCollection)
    return rel(*args, **kwargs)
