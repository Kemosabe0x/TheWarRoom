"""Worker-only adapter; do not download full seasons inside MCP requests."""
from typing import Literal


class NFLReadPyAdapter:
    def player_stats(self, season: int, summary_level: Literal["week", "reg", "post", "reg+post"] = "week"):
        import nflreadpy as nfl
        return nfl.load_player_stats(seasons=[season], summary_level=summary_level)

    def play_by_play(self, season: int):
        import nflreadpy as nfl
        return nfl.load_pbp(seasons=[season])

    def nextgen(self, season: int, stat_type: Literal["passing", "rushing", "receiving"]):
        import nflreadpy as nfl
        return nfl.load_nextgen_stats(seasons=[season], stat_type=stat_type)
