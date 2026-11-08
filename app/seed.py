"""Explicit, idempotent local demo seed: python -m app.seed."""
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from app.config import Settings
from app.models import Integration, Tenant


def main():
    engine = create_engine(Settings().database_url)
    with Session(engine) as session, session.begin():
        if session.scalar(select(Integration.id).where(Integration.external_key == "demo-store-01")):
            return
        tenant = Tenant(name="Demo store")
        session.add(tenant)
        session.flush()
        session.add(Integration(tenant_id=tenant.id, external_key="demo-store-01"))
    engine.dispose()


if __name__ == "__main__":
    main()
