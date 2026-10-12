"""Small public read adapter; larger player dictionaries belong in workers."""
import asyncio
from time import monotonic

import httpx
from pydantic import Field, ValidationError

from war_room.contracts import Contract, Snapshot
from war_room.storage import DataUnavailable


class League(Contract):
    league_id: str
    name: str
    sport: str
    season: str
    status: str
    scoring_settings: dict[str, float] = Field(default_factory=dict)


class SleeperAdapter:
    def __init__(self, client: httpx.AsyncClient):
        self.client = client
        self._cache: dict[str, tuple[float, Snapshot[League]]] = {}
        self._lock = asyncio.Lock()

    async def league(self, league_id: str) -> Snapshot[League]:
        async with self._lock:
            cached = self._cache.get(league_id)
            if cached and monotonic() - cached[0] < 60:
                return cached[1].model_copy(deep=True)
            try:
                response = await self.client.get(f"https://api.sleeper.app/v1/league/{league_id}")
                response.raise_for_status()
                payload = response.json()
                league = League.model_validate({k: payload[k] for k in League.model_fields if k in payload})
                if league.league_id != league_id or league.sport != "nfl":
                    raise DataUnavailable("Sleeper league identity or sport mismatch")
            except (httpx.HTTPError, ValueError, TypeError, ValidationError) as exc:
                raise DataUnavailable("Sleeper league request failed or returned an invalid schema") from exc
            result = Snapshot[League](status="observed", source="sleeper:league", data=league,
                                      warnings=["Fetched league settings, not live player statistics or projections."])
            if len(self._cache) >= 100:
                self._cache.pop(next(iter(self._cache)))
            self._cache[league_id] = (monotonic(), result)
            return result.model_copy(deep=True)
