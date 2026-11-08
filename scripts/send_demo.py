"""Send a signed demo event: python -m scripts.send_demo."""
import hashlib
import hmac
import json
import urllib.request

from app.config import Settings

body = json.dumps({
    "event_id": "evt_001", "type": "order.created", "order_id": "ord_1042",
    "resource_version": 1, "occurred_at": "2026-11-07T18:00:00Z", "data": {"item_count": 3},
}).encode()
signature = hmac.new(Settings().webhook_secret.get_secret_value().encode(),
                     body, hashlib.sha256).hexdigest()
request = urllib.request.Request("http://127.0.0.1:8000/api/v1/webhooks/orders", data=body,
                                 headers={"Content-Type": "application/json",
                                          "X-Integration-Id": "demo-store-01",
                                          "X-Event-Id": "evt_001",
                                          "X-Signature-SHA256": signature})
with urllib.request.urlopen(request) as response:
    print(response.status, response.read().decode())
