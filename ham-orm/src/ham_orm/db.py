from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import declarative_base, sessionmaker, Session
from contextvars import ContextVar
from typing import Any
from sqlalchemy import event


def wrap_session_begin(db: Session, *, close_on_with_exit: bool = True) -> Session:
    """
    Patch this Session instance's .begin() so:
      - You can detect usage style via db.info['tx_via_with'] (True inside `with`, False otherwise)
      - The Session auto-closes when the OUTERMOST `with db.begin():` exits (optional, default True)
      - Non-context usage `tx = db.begin()` behaves normally (no auto-close)

    Notes:
      - Only affects THIS db instance.
      - Does not change Python scoping rules; it just calls db.close() on the outermost exit.
    """
    original_begin = db.begin  # bound method
    DEPTH_KEY = "_wrap_begin_ctx_depth"

    def _inc_depth():
        depth = int(db.info.get(DEPTH_KEY, 0)) + 1
        db.info[DEPTH_KEY] = depth
        return depth

    def _dec_depth():
        depth = int(db.info.get(DEPTH_KEY, 0)) - 1
        if depth < 0:
            depth = 0
        db.info[DEPTH_KEY] = depth
        return depth

    def _wrapped_begin(*args: Any, **kwargs: Any):
        class _BeginProxy:
            def __init__(self):
                self._trans = None  # real SessionTransaction

            def _ensure_trans(self):
                if self._trans is None:
                    # default False; __enter__ will flip to True for 'with' usage
                    db.info['tx_via_with'] = db.info.get('tx_via_with', False)
                    self._trans = original_begin(*args, **kwargs)
                return self._trans

            # Context-manager path
            def __enter__(self):
                db.info['tx_via_with'] = True
                _inc_depth()
                return self._ensure_trans().__enter__()

            def __exit__(self, exc_type, exc, tb):
                try:
                    return self._ensure_trans().__exit__(exc_type, exc, tb)
                finally:
                    # Close session only when OUTERMOST begin-context exits
                    depth = _dec_depth()
                    if close_on_with_exit and depth == 0:
                        try:
                            db.close()
                        except Exception:
                            # don't let close errors mask the original exception
                            pass

            # Delegate everything else (non-with usage) to the real transaction
            def __getattr__(self, name):
                return getattr(self._ensure_trans(), name)

            def __repr__(self):
                return f"<_BeginProxy tx_via_with={db.info.get('tx_via_with')} trans={self._trans!r} depth={db.info.get(DEPTH_KEY, 0)}>"

        proxy = _BeginProxy()

        # Non-with path: start immediately and mark False
        db.info['tx_via_with'] = False
        proxy._ensure_trans()   # create the real transaction now
        return proxy

    db.begin = _wrapped_begin  # monkey-patch this Session instance
    return db

class Database:
    _instance = None
    _engine = None
    _session_factory = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(Database, cls).__new__(cls)
        return cls._instance

    def __init__(self, url: str = None, **kwargs):
        """
        Initialize SQLAlchemy engine and session factory.
        Only runs once because of the singleton pattern.
        """
        if not hasattr(self, "_engine") or self._engine is None:
            if url is None:
                raise ValueError("Database URL must be provided on first initialization")
            self._engine = create_engine(url, **kwargs)
            self._session_factory = sessionmaker(
                bind=self._engine,
                future=True,
                autoflush=False,
                autocommit=False,
            )

    def get_db(self):
        """Dependency-style DB session (with yield)."""
        db = self._session_factory()
        try:
            yield db
            db.commit()
        except:
            db.rollback()
            raise
        finally:
            db.close()

    def new_session(self) -> Session:
        """Get a simple database session without context management."""
        return wrap_session_begin(self._session_factory())

    def raw_session(self) -> Session:
        """Return a raw session bound directly to the engine."""
        return wrap_session_begin(Session(self._engine))

    async def init_db(self):
        """Initialize database tables."""
        Base.metadata.create_all(bind=self._engine)

    async def close(self):
        """Close database connections."""
        if self._engine:
            self._engine.dispose()

Base = declarative_base()

# Event hook for Base models
@event.listens_for(Base, "load", propagate=True)
def _bind_on_load(target, context):
    if hasattr(target, "bind"):
        target.bind(context.session)            


