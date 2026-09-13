"""Separate asynchronous sessions; no synchronous DB operations on the v2 event loop."""
import os
import time
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.exc import OperationalError, TimeoutError as PoolTimeout
from sqlalchemy.pool import NullPool
from ..config import DATABASE_URL


class BoundedSession(AsyncSession):
    async def _guard(self, operation, *args, **kwargs):
        from . import runtime
        if getattr(self, "_guard_depth", 0): return await operation(*args, **kwargs)
        started = time.monotonic()
        kind = "ledger" if self.bind is ledger_engine else "database"
        breaker = runtime.breakers.get(kind)
        if breaker: breaker.acquire()
        self._guard_depth = 1
        try:
            result = await operation(*args, **kwargs)
            if breaker: breaker.success()
            return result
        except (OperationalError, PoolTimeout):
            if breaker: breaker.failure()
            raise
        except BaseException:
            if breaker: breaker.release_probe()
            raise
        finally:
            self._guard_depth = 0
            budget = runtime.active_budget.get()
            if budget:
                key = kind + "_ms"
                budget.timings[key] = budget.timings.get(key, 0) + int((time.monotonic() - started) * 1000)

    async def execute(self, *a, **k): return await self._guard(super().execute, *a, **k)
    async def scalar(self, *a, **k): return await self._guard(super().scalar, *a, **k)
    async def scalars(self, *a, **k): return await self._guard(super().scalars, *a, **k)
    async def get(self, *a, **k): return await self._guard(super().get, *a, **k)
    async def flush(self, *a, **k): return await self._guard(super().flush, *a, **k)

def make_engine(url):
    pooling = {"poolclass": NullPool} if os.getenv("V2_TESTING") == "1" else {
        "pool_size": 8, "max_overflow": 4, "pool_timeout": 2}
    return create_async_engine(url, pool_pre_ping=True, **pooling, hide_parameters=True,
                               connect_args={"connect_timeout": 3,
                                             "options": "-c statement_timeout=5000 -c lock_timeout=2000"})

engine = make_engine(DATABASE_URL)
Session = async_sessionmaker(engine, class_=BoundedSession, expire_on_commit=False)
ledger_engine = make_engine(os.getenv("MODEL_LEDGER_DATABASE_URL", DATABASE_URL))
Ledger = async_sessionmaker(ledger_engine, class_=BoundedSession, expire_on_commit=False)
