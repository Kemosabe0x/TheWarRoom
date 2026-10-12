from war_room.contracts import ComboEvaluation, ComboScenario, MarketQuery, MarketQuote, Snapshot


def evaluate_combo(scenario: ComboScenario) -> ComboEvaluation:
    cost = scenario.total_cost + scenario.total_fees
    ev = scenario.joint_probability * scenario.total_payout_if_all_win - cost
    return ComboEvaluation(expected_profit=ev, maximum_loss=cost, return_on_cost=ev / cost,
        currency=scenario.currency, probability_basis=scenario.probability_basis,
        warnings=["Caller-supplied assumptions, not a model prediction or executable quote.",
                  "Payout includes returned stake; loss payout is zero. Fees are assumed unconditional.",
                  "Native combo availability is unverified; a basket of singles has different payouts."])


class BettingService:
    """Fail-closed boundary until venue collectors and execution ledger are implemented."""
    def markets(self, query: MarketQuery) -> Snapshot[list[MarketQuote]]:
        return Snapshot(status="unavailable", source=query.venue.value,
                        warnings=["Venue collector is not configured; no live quotes are available."])
