"""Versioned, bounded transport contracts; decimal money stays exact."""
from datetime import datetime, timezone
from decimal import Decimal
from enum import StrEnum
from typing import Annotated, Generic, Literal, TypeVar

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Contract(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)


class Dataset(StrEnum):
    analytics = "analytics"
    fantasy = "fantasy"
    gm = "gm"
    gruden = "gruden"
    scout = "scout"


class Venue(StrEnum):
    kalshi = "kalshi"
    polymarket = "polymarket"


class Sport(StrEnum):
    nfl = "nfl"
    nhl = "nhl"


Identifier = Annotated[str, Field(min_length=1, max_length=160, pattern=r"^[A-Za-z0-9_.:-]+$")]
Probability = Annotated[Decimal, Field(ge=0, le=1)]
Money = Annotated[Decimal, Field(ge=0, max_digits=18, decimal_places=8)]
T = TypeVar("T")


class Snapshot(Contract, Generic[T]):
    schema_version: Literal["1.0"] = "1.0"
    status: Literal["legacy_unverified", "unavailable", "observed"]
    source: str
    retrieved_at: AwareDatetime = Field(default_factory=utcnow)
    source_updated_at: AwareDatetime | None = None
    snapshot_id: str | None = None
    warnings: list[str] = Field(default_factory=list)
    data: T | None = None


class DatasetQuery(Contract):
    dataset: Dataset
    offset: int = Field(default=0, ge=0, le=1000000)
    limit: int = Field(default=25, ge=1, le=100)
    week: int | None = Field(default=None, ge=0, le=30)
    player_name: str | None = Field(default=None, min_length=1, max_length=100)
    expected_snapshot_id: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")


class DatasetPage(Contract):
    dataset: Dataset
    columns: list[str]
    rows: list[dict[str, str | None]]
    matching_rows: int
    next_offset: int | None


class DatasetInfo(Contract):
    dataset: Dataset
    rows: int
    columns: list[str]
    snapshot_id: str
    seasons_observed: list[str]


class MarketQuery(Contract):
    venue: Venue
    asset: str = Field(default="BTC", pattern=r"^[A-Z0-9]{2,12}$")
    interval_minutes: Literal[15] = 15
    limit: int = Field(default=25, ge=1, le=100)


class MarketQuote(Contract):
    venue: Venue
    market_id: Identifier
    outcome_id: Identifier
    outcome: str
    currency: str
    bid: Probability | None
    ask: Probability | None
    observed_at: AwareDatetime
    received_at: AwareDatetime
    closes_at: AwareDatetime
    rules_hash: str
    settlement_source: str
    sequence: int | None = None


class ComboLeg(Contract):
    venue: Venue
    market_id: Identifier
    outcome_id: Identifier


class OracleObservation(Contract):
    venue: Venue
    market_id: Identifier
    provider: str
    symbol: str
    reference_price: Money
    source_timestamp: AwareDatetime
    received_at: AwareDatetime
    measured_clock_offset_ms: Decimal | None = None
    sequence: int | None = None
    rules_hash: str


class ComboScenario(Contract):
    """Caller assumptions for ONE all-legs-win contract, never a basket of singles."""
    legs: list[ComboLeg] = Field(min_length=2, max_length=10)
    joint_probability: Probability
    probability_basis: str = Field(min_length=10, max_length=1000)
    total_cost: Money
    total_payout_if_all_win: Money
    total_fees: Money = Decimal("0")
    currency: Literal["USD", "USDC", "pUSD"] = "USD"

    @model_validator(mode="after")
    def validate_scenario(self):
        keys = [(x.venue, x.market_id, x.outcome_id) for x in self.legs]
        if len(keys) != len(set(keys)):
            raise ValueError("Duplicate combo leg")
        if self.total_cost <= 0 or self.total_payout_if_all_win <= 0:
            raise ValueError("Cost and payout must be positive")
        return self


class ComboEvaluation(Contract):
    status: Literal["hypothetical"] = "hypothetical"
    expected_profit: Decimal
    maximum_loss: Decimal
    return_on_cost: Decimal
    currency: str
    probability_basis: str
    warnings: list[str]


class ComboExecution(Contract):
    """Reserved action contract: identifiers must resolve to server-owned records."""
    plan_id: Identifier
    idempotency_key: Identifier
    max_total_cost: Money
    expires_at: AwareDatetime


class ExecutionReceipt(Contract):
    plan_id: str
    state: Literal["submitted", "partially_filled", "filled", "cancelled", "unknown"]
    order_ids: list[str]


class Projection(Contract):
    sport: Sport
    player_id: Identifier
    event_id: Identifier
    metric: str
    mean: Decimal
    lower: Decimal
    upper: Decimal
    model_version: str
    feature_snapshot_id: str
    as_of: AwareDatetime


class ContentRequest(Contract):
    topic: str = Field(min_length=1, max_length=300)
    persona: Dataset = Dataset.analytics
    format: Literal["social", "blog", "strategy"] = "social"
    week: int | None = Field(default=None, ge=0, le=30)


class ContentBrief(Contract):
    topic: str
    format: str
    evidence: Snapshot[DatasetPage]
    instructions: list[str]
    publication_allowed: Literal[False] = False


class NewsItem(Contract):
    source_id: str
    title: str
    canonical_url: str
    published_at: AwareDatetime
    retrieved_at: AwareDatetime
    entity_ids: list[str]
    correction_of: str | None = None
