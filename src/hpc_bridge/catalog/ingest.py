# src/hpc_bridge/catalog/ingest.py
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import yaml

from .bundled import BundledCatalog
from .entry import worker_env_problems, worker_env_raw_problems


def ingest(index_id: str, seed_path: str | Path, client) -> int:
    """Validate every seed entry against CatalogEntry and upsert them as GMetaEntries.

    Idempotent: keyed by subject (<facility_key>:<id>), so re-running overwrites in place.
    Returns the number of entries ingested. Run by a curator holding the index writer role.
    """
    catalog = BundledCatalog(Path(seed_path))  # construction re-validates every entry
    # Clients read worker_env leniently (a newer strategy must not drop the facility for older plugins): the strict
    # check is the curator's, here, before anything reaches the index.
    raw = {row.get("id"): (row.get("compute") or {}).get("worker_env")
           for row in (yaml.safe_load(Path(seed_path).read_text()) or []) if isinstance(row, dict)}
    bad = {e.id: probs for e in catalog.entries()
           if (probs := worker_env_raw_problems(raw.get(e.id)) + worker_env_problems(e.compute))}
    if bad:
        raise ValueError("refusing to ingest — worker_env problems: "
                         + "; ".join(f"{eid}: {', '.join(probs)}" for eid, probs in bad.items()))
    gmeta = [
        {
            "subject": entry.subject,
            "visible_to": ["public"],  # TODO(curator): per-entry/--visible-to for group-restricted machines (spec §6)
            "content": json.loads(entry.model_dump_json()),
        }
        for entry in catalog.entries()
    ]
    doc = {"ingest_type": "GMetaList", "ingest_data": {"gmeta": gmeta}}
    client.ingest(index_id, doc)
    return len(gmeta)


def delete_subjects(index_id: str, subjects: list[str], client) -> int:
    """Remove entries by subject (`<facility_key>:<id>`) — an id rename or a retired entry (ingest is an upsert
    by subject, so the old subject would otherwise stay listed). Returns the number of delete calls made."""
    for subject in subjects:
        client.delete_subject(index_id, subject)
    return len(subjects)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="hpc-bridge-catalog",
        description="Validate seed YAML and ingest to a Globus Search index; optionally delete subjects.",
    )
    parser.add_argument("index_id", help="target Globus Search index UUID")
    parser.add_argument("seed_path", nargs="?", help="seed .yaml file or directory (omit to only delete)")
    parser.add_argument("--delete-subject", action="append", default=[], metavar="FACILITY_KEY:ID",
                        help="remove this subject from the index (repeatable); runs BEFORE the ingest")
    args = parser.parse_args(argv)
    if not args.seed_path and not args.delete_subject:
        parser.error("give a seed_path, --delete-subject, or both")

    from globus_compute_sdk import Client
    from globus_sdk import SearchClient
    from globus_sdk.scopes import SearchScopes

    # Interactive curator path, so an interactive login is correct. SearchClient(app=...) registers
    # only the READ scope (:search); ingest is a WRITE, so additionally require :all (the Compute app
    # holds neither by default — spec §8). login() grants both in one consent; after that the token
    # is cached and the server-side SearchCatalog reads work without prompting.
    app = Client().app
    if app is None:
        raise SystemExit("hpc-bridge-catalog: the Compute SDK returned no GlobusApp (is globus-compute-sdk installed with app support?)")  # noqa: E501
    client = SearchClient(app=app)
    app.add_scope_requirements({SearchScopes.resource_server: SearchScopes.all})
    if app.login_required():
        app.login()
    if args.delete_subject:
        d = delete_subjects(args.index_id, args.delete_subject, client)
        print(f"deleted {d} subject{'' if d == 1 else 's'} from {args.index_id}: {', '.join(args.delete_subject)}",
              file=sys.stderr)
    if args.seed_path:
        n = ingest(index_id=args.index_id, seed_path=args.seed_path, client=client)
        print(f"ingested {n} entr{'y' if n == 1 else 'ies'} to {args.index_id}", file=sys.stderr)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
