def mixin_into(target_cls, source_cls):
    for name, value in source_cls.__dict__.items():
        if not name.startswith("__"):
            setattr(target_cls, name, value)
    return target_cls

def attach_base(child_cls):
    from .model import AppBaseModel
    return mixin_into(child_cls, AppBaseModel)
