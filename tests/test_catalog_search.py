# tests/test_catalog_search.py
import json

from hpc_bridge.catalog.search import SearchCatalog
from tests.fakes import fake_entry


def _gmeta(entry):
    # Mimic a Globus Search get_subject response: entry content under entries[].content
    return {"entries": [{"content": json.loads(entry.model_dump_json())}]}


class _FakeSearchClient:
    def __init__(self, subjects=None, fail=False):
        self._subjects = subjects or {}
        self._fail = fail
        self.calls = []

    def get_subject(self, index_id, subject):
        self.calls.append((index_id, subject))
        if self._fail:
            raise RuntimeError("search offline")
        if subject not in self._subjects:
            raise RuntimeError("404: subject not found")  # the real Search API 404s, not empty
        return _gmeta(self._subjects[subject])

    def post_search(self, index_id, query):
        self.calls.append((index_id, query))
        if self._fail:
            raise RuntimeError("search offline")
        return {"gmeta": [{"entries": [{"content": json.loads(e.model_dump_json())}]}
                          for e in self._subjects.values()]}


async def test_search_get_hits_live_and_writes_through_cache(tmp_path):
    e = fake_entry(id="anvil", facility_key="purdue")
    client = _FakeSearchClient(subjects={"purdue:anvil": e})
    c = SearchCatalog(index_id="idx", client=client, cache_dir=tmp_path)
    got = await c.get("purdue:anvil")
    assert got.id == "anvil"
    assert client.calls == [("idx", "purdue:anvil")]
    assert (tmp_path / "purdue%3Aanvil.json").exists()  # write-through


async def test_search_falls_back_to_cache_on_error(tmp_path):
    e = fake_entry(id="anvil", facility_key="purdue")
    (tmp_path / "purdue%3Aanvil.json").write_text(e.model_dump_json())  # prime the cache
    client = _FakeSearchClient(fail=True)
    c = SearchCatalog(index_id="idx", client=client, cache_dir=tmp_path)
    got = await c.get("purdue:anvil")
    assert got.id == "anvil"  # served from cached index data, not the failing client


async def test_search_error_without_cache_is_reported_not_swallowed(tmp_path):
    # review 2026-09-03: a registry OUTAGE used to read as "not in the catalog" (every exception was a miss)
    # — with no offline cache to serve, the transport error propagates so connect can say the registry (a Globus Search index) is unavailable
    import pytest
    c = SearchCatalog(index_id="idx", client=_FakeSearchClient(fail=True), cache_dir=tmp_path)
    with pytest.raises(RuntimeError, match="search offline"):
        await c.get("purdue:anvil")


async def test_search_miss_returns_none(tmp_path):
    client = _FakeSearchClient(subjects={})
    c = SearchCatalog(index_id="idx", client=client, cache_dir=tmp_path)
    assert await c.get("purdue:absent") is None


async def test_search_discover_maps_summaries(tmp_path):
    e = fake_entry(id="anvil", facility_key="purdue")
    client = _FakeSearchClient(subjects={"purdue:anvil": e})
    c = SearchCatalog(index_id="idx", client=client, cache_dir=tmp_path)
    got = await c.discover("anvil")
    assert {s.id for s in got} == {"anvil"}


async def test_search_discover_transport_error_propagates(tmp_path):
    # review 2: discover used to swallow everything, which made list_facilities' bug re-raise path dead.
    # A transport failure now propagates; the TOOL layer (server._list_facilities) turns it into [] and
    # re-raises anything that is not a transport error.
    import pytest
    c = SearchCatalog(index_id="idx", client=_FakeSearchClient(fail=True), cache_dir=tmp_path)
    with pytest.raises(RuntimeError, match="search offline"):
        await c.discover("anything")


async def test_search_get_resolves_a_bare_id_via_search(tmp_path):
    # connect_facility("anvil") should resolve, not only the full subject "purdue:anvil"
    # (the live-test agent tried the bare id first and got not_found).
    e = fake_entry(id="anvil", facility_key="purdue")  # subject "purdue:anvil"
    client = _FakeSearchClient(subjects={"purdue:anvil": e})  # get_subject("anvil") -> miss
    c = SearchCatalog(index_id="idx", client=client, cache_dir=tmp_path)
    got = await c.get("anvil")
    assert got is not None and got.subject == "purdue:anvil"
    # the full subject still resolves directly (no extra search)
    assert (await c.get("purdue:anvil")).id == "anvil"


async def test_one_unparseable_index_entry_does_not_blank_the_catalog(tmp_path):
    # rollout note from the final review: the shared index now holds a MEP entry (ssh_host: null) that
    # an OLDER client's schema can't parse; a per-entry guard keeps discover()/by-id working around it.
    import json as _json

    from hpc_bridge.catalog.search import SearchCatalog
    from tests.fakes import fake_entry

    good = fake_entry(id="anvil", facility_key="purdue")
    bad = {"id": "broken", "facility_key": "x"}  # fails CatalogEntry validation

    class C:
        def get_subject(self, index_id, subject):
            raise RuntimeError("404")

        def post_search(self, index_id, query):
            return {"gmeta": [{"entries": [{"content": bad}]},
                              {"entries": [{"content": _json.loads(good.model_dump_json())}]}]}

    cat = SearchCatalog("idx", C(), tmp_path)
    assert [s.id for s in await cat.discover("")] == ["anvil"]
    assert (await cat.get("anvil")).id == "anvil"  # the by-id search skips the bad hit
