"""Stranger's walk, step 1 — with NO configuration at all: `list_facilities` must work out of the box
(the public registry id is built into the plugin and read anonymously), and the agent must tell the
user how each facility is reached (`access` / `access_note`: SSH account + key vs a facility-run
multi-user endpoint that maps your Globus identity) — WITHOUT connecting, provisioning or running
anything. Cheap: no cluster block, no SSH by the agent. Reads what the agent SAID (Trace.texts).

The facilities it must relay come from its OWN `list_facilities` result (`listing_relayed`), not a snapshot, so a
facility the answer leaves out fails it whatever the registry holds. Needs the PUBLIC registry: a fake profile with a
local catalog (`mep`: HPC_BRIDGE_CATALOG_FILE replaces the registry) lists only its own MEPs, so REQUIRES skips it there.
"""
from invariants import listing_relayed, no_connect_unprompted

PROMPT = (
    "I'm new to this. Which HPC facilities can I use through hpc-bridge, and what would I need to "
    "have for each one before I could use it? Just tell me — do NOT connect to anything, do not "
    "provision or run anything. This is an automated run: there is no human to answer questions, "
    "so answer and stop."
)
USER_GOAL = ""      # autonomous
PERSONA = None
KIND = "regression"
SUMMARY = "zero config: list_facilities works out of the box; the agent says how each facility is reached and does nothing else"
TAGS = ["stranger", "registry", "cheap", "no-cluster"]
MAX_TURNS = 8
REQUIRES = {"catalog": "registry"}   # the public registry's entries, not a profile's local catalog

facilities_and_access_relayed = listing_relayed("facilities_and_access_relayed")
EXTRA_INVARIANTS = [no_connect_unprompted, facilities_and_access_relayed]
EXPECT_OK = ["agent_engaged", "no_connect_unprompted", "facilities_and_access_relayed"]
TEARDOWN = "delete"   # nothing to delete; harmless
