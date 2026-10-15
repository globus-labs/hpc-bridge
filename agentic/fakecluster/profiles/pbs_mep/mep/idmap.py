#!/usr/bin/env python3
"""identity_mapping.json for the fake MEP: ONE exact-match mapping, Globus username -> local account. Built as data
and serialised (re.escape's backslashes must be JSON-escaped); no ^/$ anchors — the mapper adds its own and would
escape ours into literals (the globus1 lesson). Usage: idmap.py <globus username> <local user>"""
import json
import re
import sys

identity, user = sys.argv[1], sys.argv[2]
print(json.dumps([{"DATA_TYPE": "expression_identity_mapping#1.0.0",
                   "mappings": [{"source": "{username}", "match": re.escape(identity), "output": user}]}], indent=2))
