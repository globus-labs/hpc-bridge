"""Stranger's walk, step 1 — with NO configuration at all: `list_facilities` must work out of the box
(the public registry id is built into the plugin and read anonymously), and the agent must tell the
user how each facility is reached (`access` / `access_note`: SSH account + key vs a facility-run
multi-user endpoint that maps your Globus identity) — WITHOUT connecting, provisioning or running
anything. Cheap: no cluster block, no SSH by the agent. Reads what the agent SAID (Trace.texts).
"""
from invariants import no_connect_unprompted, texts_mention

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

facilities_and_access_relayed = texts_mention(
    "facilities_and_access_relayed", "anvil", ("globus-labs", "globus labs"), "ssh",
    any_of=("zero ssh", "zero-ssh", "multi-user", "multi user", "identity", "no ssh", "no-ssh"),
)
EXTRA_INVARIANTS = [no_connect_unprompted, facilities_and_access_relayed]
EXPECT_OK = ["agent_engaged", "no_connect_unprompted", "facilities_and_access_relayed"]
TEARDOWN = "delete"   # nothing to delete; harmless
