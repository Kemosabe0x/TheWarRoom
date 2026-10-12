"""Read-only compatibility bridge. Production workers will replace this repository."""
import csv
import hashlib
import io
from functools import lru_cache
from pathlib import Path

from war_room.contracts import Dataset, DatasetInfo, DatasetPage, DatasetQuery, Snapshot


class DataUnavailable(ValueError):
    pass


class LegacySnapshots:
    def __init__(self, directory: Path):
        self.directory = directory.resolve()

    @lru_cache(maxsize=10)
    def _load(self, dataset: Dataset, mtime_ns: int, size: int):
        path = self.directory / f"agent_{dataset.value}_kb.csv"
        payload = path.read_bytes()
        reader = csv.DictReader(io.StringIO(payload.decode("utf-8-sig")))
        rows = tuple(dict(row) for row in reader)
        return reader.fieldnames or [], rows, hashlib.sha256(payload).hexdigest()

    def load(self, dataset: Dataset):
        path = self.directory / f"agent_{dataset.value}_kb.csv"
        try:
            stat = path.stat()
            if stat.st_size > 32 * 1024 * 1024:
                raise DataUnavailable("Legacy snapshot exceeds 32 MiB bridge limit")
            return self._load(dataset, stat.st_mtime_ns, stat.st_size)
        except (OSError, UnicodeError, csv.Error) as exc:
            raise DataUnavailable(f"Cannot read {dataset.value} snapshot") from exc

    def catalog(self) -> list[DatasetInfo]:
        result = []
        for dataset in Dataset:
            columns, rows, digest = self.load(dataset)
            seasons = {r.get("season") or (r.get("game_id") or "").split("_")[0] for r in rows}
            result.append(DatasetInfo(dataset=dataset, rows=len(rows), columns=columns,
                                      snapshot_id=digest, seasons_observed=sorted(seasons - {""})))
        return result

    def query(self, query: DatasetQuery) -> Snapshot[DatasetPage]:
        columns, rows, digest = self.load(query.dataset)
        if query.expected_snapshot_id and digest != query.expected_snapshot_id:
            raise DataUnavailable("Snapshot changed; restart pagination with the new snapshot_id")
        filtered = rows
        if query.week is not None:
            if "week" not in columns:
                raise DataUnavailable("This dataset has no week column")
            filtered = tuple(r for r in filtered if r.get("week") == str(query.week))
        if query.player_name is not None:
            name_column = next((c for c in ["player_display_name", "player"] if c in columns), None)
            if name_column is None:
                raise DataUnavailable("This dataset has no player name column")
            filtered = tuple(r for r in filtered if r.get(name_column, "").casefold() == query.player_name.casefold())
        end = query.offset + query.limit
        # Preserve legacy text without guessing types; bound long contract-history cells.
        page = [{k: (v[:1000] if v else None) for k, v in row.items()}
                for row in filtered[query.offset:end]]
        return Snapshot[DatasetPage](
            status="legacy_unverified", source=f"repository:data/agent_{query.dataset.value}_kb.csv",
            snapshot_id=digest,
            warnings=["Source freshness is unknown; retrieved_at is local read time, not feed time.",
                      "Legacy CSV values are text; cells are capped at 1000 characters.",
                      "Datasets may cover different seasons. Do not join by player name or row position."],
            data=DatasetPage(dataset=query.dataset, columns=columns, rows=page,
                             matching_rows=len(filtered), next_offset=end if end < len(filtered) else None),
        )
