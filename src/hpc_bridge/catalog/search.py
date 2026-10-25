# src/hpc_bridge/catalog/search.py
from __future__ import annotations

import asyncio
import json
from pathlib import Path

from .entry import CatalogEntry, CatalogSummary

# The public registry of HPC facilities (Globus Search index `hpc-bridge-test`, entries `visible_to: public`,
# read ANONYMOUSLY). Baked in so `list_facilities()` works out of the box; HPC_BRIDGE_SEARCH_INDEX overrides
# it (a private/staging registry). A purpose-named production index is an open V1 item — swap the id here.
PUBLIC_REGISTRY_INDEX = "6ff95fb8-1113-42be-a811-3d1cb5a67bd5"


def _not_found(exc: BaseException) -> bool:
    """Globus Search answers a missing subject with HTTP 404 (it does NOT return an empty list)."""
    return getattr(exc, "http_status", None) == 404 or "404" in str(exc)


class SearchCatalog:
    """Globus Search backed — the runtime catalog. There is **no bundled fallback**: a subject the
    index can't resolve returns ``None`` (a hard failure; the soft agent-discovery fallback is a
    later slice). The local cache is *fetched* index data — a write-through offline copy of what
    the index already returned, the channel's only resilience (no TTL, no hardcoded seed).
    """

    def __init__(self, index_id: str, client, cache_dir: Path) -> None:
        self._index_id = index_id
        self._client = client
        self._cache_dir = Path(cache_dir)
        self._cache_dir.mkdir(parents=True, exist_ok=True)

    def _cache_file(self, subject: str) -> Path:
        # percent-escape chars illegal in filenames on some OSes (':' on Windows, '/' everywhere)
        safe = subject.replace("%", "%25").replace("/", "%2F").replace(":", "%3A")
        return self._cache_dir / f"{safe}.json"

    async def get(self, machine_id: str) -> CatalogEntry | None:
        """Resolve a subject or a bare id. Returns None only when the registry answered "no such
        entry"; a TRANSPORT failure (network, 5xx, auth) is raised — after the offline cache has had
        its chance — so the caller can say the registry is unavailable (in the service's own words)
        instead of "not in the catalog"
        (found in review: every exception used to read as a miss)."""
        transport: Exception | None = None
        try:
            resp = await asyncio.to_thread(self._client.get_subject, self._index_id, machine_id)
            entries = resp.get("entries") or []
        except Exception as exc:  # noqa: BLE001 - a 404 is "no such subject"; anything else is transport
            if not _not_found(exc):
                transport = exc
            entries = []
        if entries:
            entry = CatalogEntry.model_validate(entries[0]["content"])  # re-validate on read
            self._remember(entry, machine_id)
            return entry
        # subject didn't resolve: try a bare id (e.g. "anvil" -> "purdue:anvil"), else offline cache
        try:
            by_id = await self._by_id(machine_id)
        except Exception as exc:  # noqa: BLE001 - transport again; the cache may still serve
            transport = transport or exc
            by_id = None
        if by_id is not None:
            return by_id
        if transport is not None:  # the registry could not answer: the offline copy is the only resilience
            cached = self._from_cache(machine_id)
            if cached is not None:
                return cached
            raise transport
        # The registry answered and does not have it: a curator's retraction must take effect, and a file
        # planted in the cache dir must not impersonate a curated entry (security review 2026-09-04, C-3).
        self._forget(machine_id)
        return None

    def _forget(self, *keys: str) -> None:
        for k in keys:
            try:
                self._cache_file(k).unlink()
            except FileNotFoundError:
                pass

    def _remember(self, entry: CatalogEntry, *keys: str) -> None:
        # write-through under EVERY name it was asked by (bare id, subject) so the offline cache hits
        # the same names the live registry resolves (found in review: id hits were cached under the
        # subject only, so a later offline get("anvil") missed)
        for k in {*keys, entry.subject, entry.id}:
            f = self._cache_file(k)
            f.write_text(entry.model_dump_json())
            f.chmod(0o600)

    async def _by_id(self, machine_id: str) -> CatalogEntry | None:
        # connect_facility("anvil") should work, not only the full subject "purdue:anvil" — match
        # the BundledCatalog/FakeCatalog convention (id resolves too). Search, then match the id.
        resp = await asyncio.to_thread(  # transport errors propagate: get() decides (cache, then raise)
            self._client.post_search, self._index_id, {"q": machine_id, "limit": 20}
        )
        for gmeta in resp.get("gmeta", []):
            for e in gmeta.get("entries") or []:
                try:
                    entry = CatalogEntry.model_validate(e["content"])
                except Exception:  # noqa: BLE001 - a hit this client's schema can't parse: skip, don't abort
                    continue
                if entry.id == machine_id:
                    self._remember(entry, machine_id)
                    return entry
        return None

    def _from_cache(self, subject: str) -> CatalogEntry | None:
        cached = self._cache_file(subject)
        if cached.exists():
            try:
                return CatalogEntry.model_validate(json.loads(cached.read_text()))
            except Exception:  # noqa: BLE001 - a transport failure lists nothing; the caller degrades honestly
                pass  # corrupt/stale cache — a hard miss, not a hardcoded fallback
        return None

    async def discover(self, query: str) -> list[CatalogSummary]:
        try:
            resp = await asyncio.to_thread(
                self._client.post_search, self._index_id, {"q": query or "*"}
            )
        except Exception as exc:
            if _not_found(exc):
                return []
            raise
        out = []
        for gmeta in resp.get("gmeta", []):
            entries = gmeta.get("entries") or []
            if not entries:
                continue
            try:
                out.append(CatalogEntry.model_validate(entries[0]["content"]).summary())
            except Exception:  # noqa: BLE001 - schema drift in ONE entry must not blank the whole list
                continue
        return out
