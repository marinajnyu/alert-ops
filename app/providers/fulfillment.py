from typing import Literal
from uuid import UUID

import httpx
from pydantic import BaseModel, ConfigDict, Field, ValidationError


class Operation(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    operation_key: str = Field(min_length=1, max_length=256, pattern=r"^\S+$")
    order_id: str = Field(min_length=1, max_length=128, pattern=r"^\S+$")
    resource_version: int = Field(strict=True, gt=0, le=9223372036854775807)
    item_count: int = Field(strict=True, gt=0, le=9223372036854775807)


class Shipment(Operation):
    shipment_id: UUID
    state: Literal["CREATED"]


class ProviderError(Exception):
    """Sanitized provider error; never contains response bodies or credentials."""


class UnknownOutcome(ProviderError):
    """Creation might have taken effect. Read back before sending another create."""


class LookupUnavailable(ProviderError):
    """Read-back could not establish remote state."""


class ProviderRejected(ProviderError):
    def __init__(self, status_code: int):
        self.status_code = status_code
        super().__init__(f"Provider rejected request (HTTP {status_code})")


class FulfillmentAdapter:
    """Synchronous simulator adapter. Owns its HTTP client; use as a context manager."""

    def __init__(self, base_url: str, api_key: str, *, transport=None):
        self.client = httpx.Client(base_url=base_url, headers={"Authorization": f"Bearer {api_key}"},
                                   timeout=5.0, follow_redirects=False, transport=transport,
                                   trust_env=False)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.client.close()

    def create_shipment(self, operation: Operation) -> Shipment:
        try:
            response = self.client.post(
                "/v1/shipments", headers={"Idempotency-Key": operation.operation_key},
                json=operation.model_dump(exclude={"operation_key"}),
            )
        except httpx.RequestError:
            raise UnknownOutcome("Creation response unavailable; read back remote state") from None
        if response.status_code in (400, 401, 403, 409, 422, 429):
            raise ProviderRejected(response.status_code)
        if response.status_code not in (200, 201):
            raise UnknownOutcome("Unexpected creation response; read back remote state")
        try:
            shipment = Shipment.model_validate(response.json())
            if shipment.model_dump(exclude={"shipment_id", "state"}) != operation.model_dump():
                raise ValueError("Unexpected shipment")
            return shipment
        except (ValueError, ValidationError):
            raise UnknownOutcome("Invalid creation response; read back remote state") from None

    def get_shipment(self, operation_key: str) -> Shipment | None:
        try:
            response = self.client.get("/v1/shipments/by-operation",
                                       params={"operation_key": operation_key})
        except httpx.RequestError:
            raise LookupUnavailable("Remote state unavailable") from None
        if response.status_code == 404:
            return None
        if response.status_code in (401, 403):
            raise ProviderRejected(response.status_code)
        if response.status_code != 200:
            raise LookupUnavailable("Unexpected lookup response")
        try:
            shipment = Shipment.model_validate(response.json())
            if shipment.operation_key != operation_key:
                raise ValueError("Unexpected operation key")
            return shipment
        except (ValueError, ValidationError):
            raise LookupUnavailable("Invalid lookup response") from None

    def verify_shipment(self, operation: Operation) -> Shipment | None:
        shipment = self.get_shipment(operation.operation_key)
        if shipment and shipment.model_dump(exclude={"shipment_id", "state"}) != operation.model_dump():
            raise LookupUnavailable("Remote shipment does not match expected operation")
        return shipment
