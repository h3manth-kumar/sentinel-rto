from enum import Enum
from dataclasses import dataclass

class RiskTier(Enum):
    ALLOW = "ALLOW"
    CHALLENGE_DEPOSIT = "CHALLENGE_DEPOSIT"
    FORCE_PREPAID = "FORCE_PREPAID"
    BLOCK = "BLOCK"

@dataclass
class PolicyDecision:
    risk_tier: str
    action_type: str
    deposit_amount_in_paise: int | None
    reason_code: str

class PolicyEvaluator:
    def __init__(self):
        self.deposit_amount = 4900

    def evaluate(self, risk_score: int, burst_action: str | None, merchant_config: dict) -> PolicyDecision:
        if risk_score <= 30:
            tier = RiskTier.ALLOW
        elif risk_score <= 70:
            tier = RiskTier.CHALLENGE_DEPOSIT
        elif risk_score <= 90:
            tier = RiskTier.FORCE_PREPAID
        else:
            tier = RiskTier.BLOCK

        if burst_action == "FORCE_PREPAID":
            tier = RiskTier.FORCE_PREPAID
        elif burst_action == "CHALLENGE" and tier == RiskTier.ALLOW:
            tier = RiskTier.CHALLENGE_DEPOSIT

        action_type = tier.value
        deposit = self.deposit_amount if tier == RiskTier.CHALLENGE_DEPOSIT else None
        reason = "NORMAL_EVALUATION"
        if burst_action:
            reason = f"BURST_OVERRIDE_{burst_action}"

        return PolicyDecision(
            risk_tier=tier.value,
            action_type=action_type,
            deposit_amount_in_paise=deposit,
            reason_code=reason
        )
