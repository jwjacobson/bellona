import uuid
from datetime import datetime

from sqlalchemy import DateTime, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


def uuid_pk() -> Mapped[uuid.UUID]:
    return mapped_column(default=uuid.uuid4, primary_key=True)


def now_utc() -> Mapped[datetime]:
    return mapped_column(DateTime(timezone=True), server_default=func.now())
