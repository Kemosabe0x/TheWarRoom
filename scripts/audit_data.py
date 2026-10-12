"""Reproduce the data inventory without fetching or modifying source data."""
import csv
import hashlib
import io
import json
from collections import Counter
from pathlib import Path


def audit(root: Path) -> list[dict]:
    inventory = []
    for path in sorted((root / "data").glob("*.csv")):
        payload = path.read_bytes()
        reader = csv.DictReader(io.StringIO(payload.decode("utf-8-sig")))
        rows = list(reader)
        fields = reader.fieldnames or []
        years = {r.get("season") or (r.get("game_id") or "").split("_")[0] for r in rows}
        name_keys = Counter((r["player_display_name"], r.get("week")) for r in rows) if "player_display_name" in fields else None
        inventory.append({"file": path.relative_to(root).as_posix(), "bytes": len(payload),
            "sha256": hashlib.sha256(payload).hexdigest(), "rows": len(rows), "columns": fields,
            "season_evidence": sorted(years - {""}),
            "weeks": sorted({int(r["week"]) for r in rows if r.get("week")}),
            "game_dates": sorted({r["game_date"] for r in rows if r.get("game_date")}),
            "duplicate_player_name_week_keys": sum(v > 1 for v in name_keys.values()) if name_keys is not None else None,
            "source_updated_at": None})
    return inventory


if __name__ == "__main__":
    print(json.dumps(audit(Path(__file__).resolve().parents[1]), indent=2))
