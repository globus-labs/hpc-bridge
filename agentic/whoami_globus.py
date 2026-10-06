#!/usr/bin/env python
"""Which Globus identity does a storage.db hold, and which scopes? — the two facts that decide
whether a run can reach a facility MEP (its identity mapping must map THIS identity) and the
catalog (the Search scope must be present).

    python agentic/whoami_globus.py                 # the default ~/.globus_compute/storage.db
    python agentic/whoami_globus.py /path/to/dir    # a dir holding storage.db (what the jail mounts)

Read-only: it never triggers a login. If the db has no Compute login it says so and exits 1.
"""
from __future__ import annotations

import os
import sqlite3
import sys
from pathlib import Path


def main(argv: list[str]) -> int:
    user_dir = Path(argv[1]).expanduser() if len(argv) > 1 else Path.home() / ".globus_compute"
    db = user_dir / "storage.db" if user_dir.is_dir() else user_dir
    if not db.exists():
        print(f"no storage.db at {db}", file=sys.stderr)
        return 1
    os.environ["GLOBUS_COMPUTE_USER_DIR"] = str(db.parent)  # the SDK finds the db here

    # 1. Scopes held, straight from the token table (resource server -> scope string).
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        rows = con.execute(
            "select resource_server, token_data_json from token_storage"
        ).fetchall()
    except sqlite3.Error as exc:  # schema drift — report, don't crash
        rows = []
        print(f"(could not read token_storage: {exc})", file=sys.stderr)
    finally:
        con.close()
    import json

    held = {}
    for rs, blob in rows:
        try:
            held[rs] = json.loads(blob).get("scope", "") or ""
        except Exception:  # noqa: BLE001
            held[rs] = ""

    # 2. Who the tokens belong to. The db already names the identity (identity_id in the token
    # data); resolve it to a username via Auth `userinfo`, using the app's EXISTING auth.globus.org
    # authorizer (silent refresh). Do NOT construct AuthClient(app=app): that registers new scope
    # requirements (profile/email) the db may not hold, and the app would then PROMPT for a login.
    from globus_compute_sdk import Client
    from globus_sdk import AuthClient

    app = Client().app
    if app.login_required():
        print(f"{db}: no complete Compute login in this db (login_required=True)", file=sys.stderr)
        return 1
    identity_id = next((json.loads(b).get("identity_id") for _rs, b in rows if b), None)
    try:
        authorizer = app.get_authorizer("auth.globus.org")
        ac = AuthClient(authorizer=authorizer)
        # `openid` alone makes userinfo return just the `sub`; the Identities API (same token)
        # resolves the id to a username + linked identities.
        ids = ac.get_identities(ids=identity_id)["identities"] if identity_id else []
        me = next(iter(ids), {})
        info = {"preferred_username": me.get("username"), "name": me.get("name"), "sub": identity_id}
    except Exception as exc:  # noqa: BLE001 - still report what the db itself says
        info = {"preferred_username": f"<lookup failed: {type(exc).__name__}: {exc}>"[:120], "sub": identity_id}
    print(f"storage.db : {db}")
    print(f"identity   : {info.get('preferred_username')}  ({info.get('name')})")
    print(f"identity id: {info.get('sub') or identity_id}")
    print("scopes held:")
    for rs in sorted(held):
        print(f"  {rs}")
        for s in held[rs].split():
            print(f"    {s}")
    has_search = any("search" in rs for rs in held)
    print(f"\ncatalog (Search scope): {'yes' if has_search else 'NO - run hpc-bridge-catalog once'}")
    print("MEP mapping: the facility must map the identity above (globus-cluster-mep maps gusellerm@uchicago.edu -> glabs-gc)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
