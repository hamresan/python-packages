from functools import wraps
from sqlalchemy.exc import SQLAlchemyError, IntegrityError

class HookFailed(RuntimeError):
    pass

def tx_wrap(*, refresh_on_commit: bool = True, return_self_on_success: bool = False):
    def decorator(main_method):
        @wraps(main_method)
        def wrapper(self, *args, **kwargs):
            db = getattr(self, "_db", None)
            if db is None:
                raise RuntimeError("No session bound")

            autocommit = self.is_autocommit_enabled()
            in_tx = bool(db.in_transaction())
            manage_tx = autocommit and not in_tx

            #print(f"tx_wrap: autocommit={autocommit} in_tx={in_tx} manage_tx={manage_tx}")

            def safe_rollback(sess):
                try: sess.rollback()
                except SQLAlchemyError: raise

            def safe_commit(sess):
                try: sess.commit()
                except Exception:
                    safe_rollback(sess)
                    raise

            try:
                entity = main_method(self, *args, **kwargs)
                #print('__entity__=', entity, "manage_tx=", manage_tx)
                if entity:
                    if manage_tx:
                        safe_commit(db)
                        if refresh_on_commit:
                            try: db.refresh(entity)
                            except SQLAlchemyError: pass
                    else: db.flush()
                    return self if return_self_on_success else entity
                if manage_tx:
                    safe_rollback(db)
                    return None
                raise HookFailed("after_* hook returned False")
            except IntegrityError:
                if manage_tx:
                    safe_rollback(db)
                    return None
                raise
            except SQLAlchemyError:
                if manage_tx:
                    safe_rollback(db)
                    return None
                raise
            except Exception:
                if manage_tx: safe_rollback(db)
                raise
        return wrapper
    return decorator
