from datetime import datetime
from pydantic import BaseModel

class ClientSignals(BaseModel):
    is_bot_keystrokes: bool
    form_fill_duration_ms: int
    canvas_entropy_score: float

class DeviceInfo(BaseModel):
    fingerprint_hash: str
    ip_address: str
    user_agent_raw: str
    client_signals: ClientSignals

class CustomerInfo(BaseModel):
    phone_hash: str
    email_domain: str
    account_age_days: int

class ShippingAddress(BaseModel):
    raw_text: str
    pincode: str
    h3_index_res9: str | None = None

class RiskEvaluateRequest(BaseModel):
    merchant_id: str
    order_id: str
    timestamp: datetime
    amount_in_paise: int
    payment_method: str
    customer: CustomerInfo
    device: DeviceInfo
    shipping_address: ShippingAddress

class ShapContributor(BaseModel):
    feature: str
    weight: float

class ActionResponse(BaseModel):
    type: str
    deposit_amount_in_paise: int | None
    reason_code: str

class ExplanationResponse(BaseModel):
    shap_contributors: list[ShapContributor]
    syndicate_detected: bool

class ExecutionMetrics(BaseModel):
    total_latency_ms: float
    redis_lookup_ms: float
    onnx_inference_ms: float

class RiskEvaluateResponse(BaseModel):
    evaluation_id: str
    order_id: str
    risk_score: int
    risk_tier: str
    action: ActionResponse
    explanation: ExplanationResponse
    execution_metrics: ExecutionMetrics

class HealthResponse(BaseModel):
    status: str
    version: str
    uptime_seconds: float
