import hashlib
import hmac
import json
import uuid
from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Response
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, ConfigDict, Field, StringConstraints
from sqlalchemy import create_engine, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker

from simulator.config import SimulatorSettings
from simulator.models import Shipment

Identifier = Annotated[str, StringConstraints(min_length=1, max_length=128, pattern=r"^\S+$")]
OperationKey = Annotated[str, StringConstraints(min_length=1, max_length=256, pattern=r"^\S+$")]


class CreateShipment(BaseModel):
    model_config = ConfigDict(extra="forbid")
    order_id: Identifier
    resource_version: int = Field(strict=True, gt=0, le=9223372036854775807)
    item_count: int = Field(strict=True, gt=0, le=9223372036854775807)


class ShipmentResponse(CreateShipment):
    shipment_id: uuid.UUID
    operation_key: OperationKey
    state: str


def representation(shipment: Shipment) -> dict:
    return {"shipment_id": shipment.id, "operation_key": shipment.operation_key,
            "order_id": shipment.order_id, "resource_version": shipment.resource_version,
            "item_count": shipment.item_count, "state": shipment.state}


def create_app(settings: SimulatorSettings | None = None) -> FastAPI:
    settings = settings or SimulatorSettings()
    engine = create_engine(settings.database_url, pool_pre_ping=True)
    sessions = sessionmaker(engine, expire_on_commit=False)
    bearer = HTTPBearer(auto_error=False)

    @asynccontextmanager
    async def lifespan(app):
        yield
        engine.dispose()

    app = FastAPI(title="AlertOps fulfillment simulator", lifespan=lifespan)
    app.state.sessions = sessions

    def account(credentials: HTTPAuthorizationCredentials | None = Depends(bearer)) -> str:
        supplied = credentials.credentials.encode() if credentials else b""
        matched = None
        for name, secret in settings.accounts.items():
            if hmac.compare_digest(supplied, secret.get_secret_value().encode()):
                matched = name
        if matched is None:
            raise HTTPException(401, "Invalid provider credentials",
                                headers={"WWW-Authenticate": "Bearer"})
        return matched

    @app.post("/v1/shipments", response_model=ShipmentResponse, status_code=201)
    def create(body: CreateShipment, response: Response,
               operation_key: OperationKey = Header(alias="Idempotency-Key"),
               account_id: str = Depends(account)):
        canonical = json.dumps(body.model_dump(), sort_keys=True, separators=(",", ":")).encode()
        digest = hashlib.sha256(canonical).digest()
        try:
            with sessions() as session, session.begin():
                shipment_id = session.scalar(insert(Shipment).values(
                    id=uuid.uuid4(), account_id=account_id, operation_key=operation_key,
                    **body.model_dump(), request_hash=digest,
                ).on_conflict_do_nothing().returning(Shipment.id))
                shipment = session.scalar(select(Shipment).where(
                    Shipment.account_id == account_id, Shipment.operation_key == operation_key
                ))
                if shipment is None or shipment.request_hash != digest:
                    raise HTTPException(409, "Operation key or order already used with different content")
                if shipment_id is None:
                    response.status_code = 200
                result = representation(shipment)
            return result
        except SQLAlchemyError:
            raise HTTPException(503, "Provider unavailable") from None

    @app.get("/v1/shipments/by-operation", response_model=ShipmentResponse)
    def read(operation_key: OperationKey = Query(), account_id: str = Depends(account)):
        try:
            with sessions() as session:
                shipment = session.scalar(select(Shipment).where(
                    Shipment.account_id == account_id, Shipment.operation_key == operation_key
                ))
                if shipment is None:
                    raise HTTPException(404, "Shipment not found")
                return representation(shipment)
        except SQLAlchemyError:
            raise HTTPException(503, "Provider unavailable") from None

    return app
