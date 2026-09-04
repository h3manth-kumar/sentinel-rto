from datetime import datetime
from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict


class KafkaTopics:
    ORDERS_RAW = 'orders.raw'
    CANCELLATIONS = 'cancellations'
    RTO_EVENTS = 'rto.events'


class OrderEvent(BaseModel):
    merchant_id: str
    order_id: str
    timestamp: datetime
    amount_in_paise: int
    payment_method: Literal['COD', 'PREPAID', 'UPI']
    customer_phone_hash: str
    device_fingerprint_hash: str
    h3_index_res9: str
    pincode: str
    risk_score: Optional[int] = None
    risk_tier: Optional[str] = None
    status: Literal['PENDING', 'DELIVERED', 'RTO', 'CANCELLED']

    model_config = ConfigDict(
        json_encoders={
            datetime: lambda v: v.isoformat()
        }
    )


class CancellationEvent(BaseModel):
    order_id: str
    merchant_id: str
    timestamp: datetime
    reason: str
    customer_phone_hash: str
    device_fingerprint_hash: str

    model_config = ConfigDict(
        json_encoders={
            datetime: lambda v: v.isoformat()
        }
    )


class RTOEvent(BaseModel):
    order_id: str
    merchant_id: str
    timestamp: datetime
    h3_index_res9: str
    customer_phone_hash: str
    device_fingerprint_hash: str
    logistics_cost_paise: int

    model_config = ConfigDict(
        json_encoders={
            datetime: lambda v: v.isoformat()
        }
    )
