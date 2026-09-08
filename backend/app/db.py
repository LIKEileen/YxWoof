from datetime import datetime, timezone
from uuid import uuid4
from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker
from .config import DATABASE_URL

def now():
    return datetime.now(timezone.utc)

def uid():
    return str(uuid4())

class Base(DeclarativeBase):
    pass

engine = create_engine(DATABASE_URL, pool_pre_ping=True, pool_size=12, max_overflow=12, pool_timeout=3,
                       connect_args={"connect_timeout":3, "options":"-c statement_timeout=5000 -c lock_timeout=3000"},
                       hide_parameters=True)
SessionLocal = sessionmaker(engine, expire_on_commit=False)
