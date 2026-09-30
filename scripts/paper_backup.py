"""Create a consistent SQLite backup, including committed WAL contents."""
import argparse
from pathlib import Path
import sqlite3

from core.config import get_settings


def backup(source: Path, destination: Path) -> None:
    # Exclusive destination creation prevents accidental replacement of another run.
    with destination.open("xb"):
        pass
    try:
        with sqlite3.connect(source.resolve().as_uri() + "?mode=ro", uri=True) as src:
            with sqlite3.connect(destination) as dst:
                src.backup(dst)
                if dst.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                    raise ValueError("Backup integrity check failed")
    except Exception:
        destination.unlink(missing_ok=True)
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("destination", type=Path)
    args = parser.parse_args()
    backup(get_settings().paper_state_path, args.destination)
    print(f"Paper backup written to {args.destination}")


if __name__ == "__main__":
    main()
