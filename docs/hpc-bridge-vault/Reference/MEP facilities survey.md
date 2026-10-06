# MEP facilities survey

> [!info] Provenance
> A web survey run by a research subagent on 2026-09-03 (prompted by the Tier-2 A registry work): which real HPC facilities operate a **facility-run Globus Compute multi-user endpoint**, with UUIDs, identity domains, template keys and the documented unmapped-identity behaviour. Every claim cites its page; verify a UUID with a canary before it enters the registry. hpc-bridge implications are in the last section.

## Survey
Researched 2026-09-03. Scope: HPC facilities that run a **facility-operated Globus Compute multi-user endpoint** (root-run manager, identity mapping to local accounts, users submit with zero SSH), as opposed to facilities that merely *support* user-run single-user endpoints. Every claim below is tied to a page that says it; where I could only find slides or third-party usage rather than the facility's own docs, that is called out.

## 1. Summary table

| # | Facility / machine | Status | MEP UUID(s) (as published) | Scheduler | Required `user_endpoint_config` | Source |
|---|---|---|---|---|---|---|
| 1 | **ALCF Polaris** | GA — "Facility supported" (docs page created 2026-03 as beta; UUIDs published 2026-07; "new MEP config" 2026-08-24) | `9a947ba5-f537-4681-acf3-cc66485aadec` | PBS Pro | `queue`, `account` (required); many optional | docs.alcf.anl.gov/services/globus-compute/ |
| 2 | **ALCF Crux** | GA — "Facility supported" | `fd8b54bb-9452-411d-8e3a-09408156a886` (current); an older UUID `d01d0c83-e570-4977-9170-1b8f2316e7c6` appears in ALCF material dated 2026-06/07 — see note | PBS Pro | `queue`, `account` (required) | same |
| 3 | **NCSA Delta** | GA — "NCSA Delta offers a Globus Compute Multi-user Endpoint" | `4a266c83-3c68-4a75-99b3-9c7459f2f7ef` | Slurm | example shows `account`, `partition`, `scheduler_options` | docs.ncsa.illinois.edu Delta "Running Jobs" |
| 4 | **NeSI / REANNZ Mahuika** (NZ) | Documented, **"early-access mode and may change"** | `reannz-login` `63c0b682-43d1-4b97-bf23-6a676dfdd8bd`; `reannz-slurm` `abf152c8-ad9b-453f-bcc8-3424284344f3` | Slurm (login-node MEP for light tasks) | `ACCOUNT_ID` (required); `WALL_TIME`, `MEM_PER_CPU`, `GPUS_PER_NODE` optional | docs.nesi.org.nz/Batch_Computing/Globus_Compute/ |
| 5 | **Purdue Anvil** (ACCESS) | **Deployed, but not in Purdue's user docs** — GlobusWorld 2025 slides say "Anyone with an active allocation can submit"; "shared" partition config "work in progress" | `5aafb4c1-27b2-40d8-a038-a0277611868f` (slides + third-party code) | Slurm | third-party usage shows `account`, `qos`, `walltime` | GlobusWorld 2025 slides; Garden-AI groundhog docs; Purdue GTAP README |
| 6 | **SDSC Expanse** (ACCESS) | **Not GA** — talk about "motivations and progress"; supports "early users"; slides say adoption "will drive multi-user endpoint deployment" | none published | Slurm | n/a | GlobusWorld 2025 talk/slides |
| — | NERSC Perlmutter, OLCF Frontier, TACC (Frontera/Stampede3/Vista), PSC Bridges-2, TAMU FASTER/ACES, IU Jetstream2, ALCF Aurora | **No documented MEP** (single-user endpoints only; Aurora explicitly "does not yet support MEPs") | — | — | — | see §3 |

No Globus page lists MEP deployments (see §5).

## 2. Documented facility MEPs — detail

### 2.1 ALCF — Polaris and Crux (Argonne Leadership Computing Facility)

- **Docs:** https://docs.alcf.anl.gov/services/globus-compute/ (rendered site returns 403 to scripted fetches; source is public at https://github.com/argonne-lcf/user-guides/blob/main/docs/services/globus-compute.md). Listed from https://docs.alcf.anl.gov/services/ as "Globus Compute: Globus Compute Platform". Companion repo: https://github.com/argonne-lcf/alcf-globus-compute (README: "Facility supported multiuser endpoints are available to all users of ALCF systems where they are supported.").
- **Availability wording:** "There are two options for using Globus Compute on ALCF systems: 1. Facility supported Globus Compute Endpoints (currently supported on Polaris and Crux). … 2. Users may run their own Globus Compute Endpoints on login nodes…". Section heading: "Facility Supported Multiuser Endpoints (MEPs) — Facility supported, multiuser endpoints are currently offered on Polaris and Crux."
- **History (git):** file created 2026-03-24 with "Multi-user endpoints are currently in beta and available for early users only."; 2026-07-21 rewrite lists UUIDs as facility supported; 2026-08-24 "updates for new MEP config on Polaris"; 2026-08-28 "Updates to crux options".
- **UUIDs:** Polaris `9a947ba5-f537-4681-acf3-cc66485aadec`; Crux `fd8b54bb-9452-411d-8e3a-09408156a886`. Both link to `https://app.globus.org/compute/endpoints/<uuid>`: "The Globus pages for these endpoints will give up-to-date details on their configuration templates, schemas, and status."
  - **Crux UUID caveat:** the 2026-07-21 draft of the same page and ALCF's agent skill file (https://github.com/argonne-lcf/alcf-agent-skillset/blob/main/globus/globus-compute-multiuser-endpoints.md, `last_verified: 2026-06`) give Crux as `d01d0c83-e570-4977-9170-1b8f2316e7c6`; from 2026-07-22 the docs use `fd8b54bb-…`. Treat `fd8b54bb-…` as current and verify with a canary before adding to a registry.
- **Aurora:** explicitly no MEP — "This is appropriate for machines that do not yet support MEPs, like Aurora".
- **Identity / auth:** first run: "Select 'Argonne LCF' from the organizations menu and you will be taken to an ALCF page where you will be asked for your ALCF username and MobilePass+ code." The identity-mapping rule itself is not published; the PEARC'24 MEP paper (by Globus/ALCF authors) uses ALCF as its worked example of an auth policy: "if deployed on a supercomputer at the Argonne Leadership Computing Facility (ALCF), the authentication domain can be set to 'alcf.anl.gov,' and authentication timeout can be set to 12 hours. The user then has to authenticate with an ALCF identity every 12 hours to submit new tasks." (Paper, not the ALCF docs — treat the 12 h as illustrative.) ALCF's agent-skill prerequisites: "A Globus Auth identity linked to an ALCF account with an active project allocation on the target system."
- **One-time steps:** none beyond an ALCF account/allocation and the interactive Globus login on first SDK use (browser or device-code). No consent/policy acceptance step is documented.
- **Scheduler & template (PBS Pro):** required `queue`, `account`. Optional (Polaris defaults): `walltime` `"1:00:00"`, `nodes_per_block` 1, `max_workers_per_node` 100, `cores_per_worker` 1, `available_accelerators`, `cpu_affinity` `"alternating"`, `max_idletime` 240, `init_blocks` 0, `min_blocks` 0, `max_blocks` 1, `launcher_type` `"SimpleLauncher"` (or `"MpiExecLauncher"`), `worker_init` (default `export TMPDIR=/tmp; export PATH=$PATH:/opt/globus-compute-agent/venv-py313/bin/`; user string is *prepended*), `scheduler_options` default `"#PBS -l filesystems=home"` ("full override REPLACES default — re-include filesystems="), `select_options` `"system=polaris"`/`"system=crux"`, `allowed_functions`, `max_retries_on_system_failure`, `drain_period`, `container_type/uri/cmd_options`. Schema has `additionalProperties: true` (unknown keys pass validation and are ignored by the template). Worker Python is 3.13; MEP parsl pin currently `2026.02.23` ("it is necessary to match the exact parsl version that is used by the MEPs"). Queue named in examples: `debug`.
- **Unmapped-identity behaviour:** not documented by ALCF (see §4 for Globus' generic behaviour).
- **Operational note relevant to hpc-bridge:** "Runaway job submission" troubleshooting — the fix requires logging in and `rm ~/.globus_compute/*/daemon.pid`, i.e. some failure modes still need SSH. UEP directories are `~/.globus_compute/uep*/`.

### 2.2 NCSA Delta

- **Docs:** https://docs.ncsa.illinois.edu/systems/delta/en/latest/user_guide/running_jobs.html — section "Launching jobs using Globus Compute" → "NCSA Delta Globus Compute Multi-user Endpoint (MEP)". (Source repo linked as github.com/ncsa/Delta_doc but it is not publicly accessible, so I could not date the addition.)
- **Availability wording:** "NCSA Delta offers a Globus Compute Multi-user Endpoint for users to launch jobs using Globus Compute." … "The MEP runs as a privileged system service and is capable of accepting function execution requests from multiple authenticated users. Jobs submitted through the MEP are dispatched under the submitting user's account and allocation… The globus compute MEP lets users submit jobs to the cluster without having to login to the cluster."
- **UUID:** "NCSA Delta MEP is available at the following URL: https://app.globus.org/compute/endpoints/4a266c83-3c68-4a75-99b3-9c7459f2f7ef and has the endpoint ID 4a266c83-3c68-4a75-99b3-9c7459f2f7ef."
- **Identity mapping:** not described on the page (which Globus identity — NCSA vs ACCESS — is required is not stated; Delta's *transfer* collections are split "NCSA Delta" (NCSA identity) vs "ACCESS Delta" (ACCESS identity), but nothing says the MEP follows that). NCSA's public Puppet profile for the compute agent shows the mechanism: `profile_globus::compute_agent::identity_mapfile: "/sw/admin/grid-security/oauth-mapfile.delta"` (a static GCS-style mapfile) with an older test endpoint id `44a4297d-d07d-41a8-8ce9-c89464b23330` named `delta_test` (https://github.com/ncsa/puppet-profile_globus/blob/main/README.md — illustrative admin config, not user docs).
- **One-time steps:** none documented beyond `pip install globus-compute-sdk` and a Delta allocation.
- **Scheduler & template (Slurm):** example `user_config = {"account": "<your-allocation-name>", "partition": "<partition-name>", "scheduler_options": "#SBATCH --gpus-per-node=1\n#SBATCH --time=00:10:00"}`. The full schema is not published; partitions are the standard Delta ones (`cpu`, `gpuA100x4`, `gpuA40x4`, `gpuH200x8`, `gpuMI100x8`, `*-interactive`, `*-preempt`). Default wall-clock on Delta is 30 min.
- **DeltaAI:** the DeltaAI running-jobs page has no MEP section.
- **Unmapped identity:** not documented.

### 2.3 NeSI / REANNZ — Mahuika (New Zealand)

- **Docs:** https://docs.nesi.org.nz/Batch_Computing/Globus_Compute/ (source: https://github.com/nesi/support-docs/blob/main/docs/Batch_Computing/Globus_Compute.md, present since at least 2025-12). First public mention: GlobusWorld 2024 talk "First Steps with Globus Compute Multi-User Endpoints" (Chris Scott, NeSI) — then "Early stages of testing the multi-user endpoint".
- **Status wording:** "Warning: Our Globus Compute offering is in early-access mode and may change." "On Mahuika, we host a Globus Compute multi-user endpoint, allowing users to submit and manage compute tasks through Globus or compatible clients without needing to connect to the HPC via SSH or OnDemand."
- **UUIDs:** `reannz-login` `63c0b682-43d1-4b97-bf23-6a676dfdd8bd` — "Lightweight tasks that are suitable to be run on a login node, such as submitting Slurm jobs, checking job status, etc."; `reannz-slurm` `abf152c8-ad9b-453f-bcc8-3424284344f3` — "Resource intensive tasks; work sent to this endpoint will run in a Slurm job". (This is the same "login shape + compute shape" split hpc-bridge uses.)
- **Identity mapping:** "a user identity mapping process maps the user's Globus identity to their account on Mahuika"; "users must have a Globus account … and they must add a linked identity with the 'NeSI Keycloak' to their Globus account". The linked identity is `<username>@iam.nesi.org.nz` (NeSI Globus quick-start; RemoteJobManager sets `session_required_single_domain = "iam.nesi.org.nz"`).
- **One-time steps:** "You must have a REANNZ HPC account, Globus account and have linked an identity from your Globus account to the NeSI Keycloak. This can be achieved by navigating to the HPC Storage in the Globus web app and ensuring you can see your REANNZ HPC files." (i.e. a linked-identity step, no separate consent.)
- **Template (Slurm):** `ACCOUNT_ID` (required, project code); `WALL_TIME` (default `00:05:00`); `MEM_PER_CPU` (default `2G`); `GPUS_PER_NODE`. Limits: "Currently limited to a single CPU", "You must use Python 3.11", packages limited to the `Python/3.11.6-foss-2023a` module, "lag of around 1 minute … if you have not used the endpoint recently", SDK pinned `"globus_compute_sdk>=3,<4"`, "Globus Compute version 3.x".
- **Unmapped identity:** not documented.

### 2.4 Purdue Anvil (NSF ACCESS) — deployed, documented only in slides / third-party code

- **Primary source:** GlobusWorld 2025 slides "Globus at Purdue: Globus for protected data and GCE and Anvil" (Preston Smith, Geoffrey Lentner, Charles Christoffer; 2025-04-23) https://g-1f310f.ac84b1.a567.data.globus.org/globusworld_2025/public/p_691e4a0d-3b68-4539-a01c-27e7a4aa701a.pdf — slide "Anvil Multi-User Globus Compute Endpoint — Enabling GCE for ACCESS users on Anvil": "Anyone with an active allocation can submit: 5aafb4c1-27b2-40d8-a038-a0277611868f"; "Mapped to local accounts on Anvil and scheduled via Slurm."; "Configured as whole-node on 'shared' partition (work in progress)."; "Build apps with Globus SDK to take advantage of ACCESS compute resources!"
- **Talk abstract** (https://www.globusworld.org/conf/2025.php): "Following the general availability of Globus Compute Engine with multi-user support we deployed GCE on Anvil (and soon all campus clusters) allowing for researchers to target Anvil as a compute endpoint and Globus Flows action provider using their ACCESS credentials." → identity domain is ACCESS (`@access-ci.org`), by the facility's own description.
- **No Purdue user-doc page:** the RCAC docs sitemap (docs.rcac.purdue.edu) only has `…/storage/globus/` pages; the Anvil knowledge base has no "Globus Compute" text; the ACCESS "Globus Compute on ACCESS" Confluence page (https://access-ci.atlassian.net/wiki/spaces/ACCESSdocumentation/pages/552828929/Globus+Compute+on+ACCESS) still says "To use Globus Compute on a resource provider each researcher must first deploy their own Globus Compute endpoint" and lists Anvil, Bridges-2, Delta, Expanse, FASTER, Stampede3 as *single-user* example configs only.
- **Third-party corroboration that the UUID is live and usable:** Garden-AI "groundhog" ships `anvil` as a pre-configured endpoint name with `endpoint = "5aafb4c1-27b2-40d8-a038-a0277611868f"`, `account = "…"`, `qos = "cpu"`, `walltime = "00:30:00"` (https://github.com/Garden-AI/groundhog/blob/main/docs/examples/configuration.md); a Purdue GTAP project README (https://github.com/junghawoo/globus_compute) says "Anvil's Globus Multi-User Endpoint (MEP) runs Python 3.13" and uses the same UUID with a community account.
- **Template keys seen in the wild:** `account`, `qos`, `walltime` (Slurm). No published schema.

### 2.5 SDSC Expanse — early users only, no published endpoint

- GlobusWorld 2025 talk "The Realities of Enabling Programmatic Access" (Rick Wagner, SDSC CTO; slides https://g-1f310f.ac84b1.a567.data.globus.org/globusworld_2025/public/p_951f926f-171a-4ff8-ad45-76fde92aad63.pdf). Abstract: "the motivations and progress around deploying Globus Compute Multi-Users Endpoints on SDSC's Expanse system … I'll present how we've addressed these challenges to support early users of Globus Compute on Expanse." Slides: "Adding Globus Compute Multi-user Requires Resources — Hardware: One or more dedicated servers … Labor …"; "Single-user endpoints can run on login nodes (has drawbacks) … Increased adoption will drive multi-user endpoint deployment."
- HPCwire/Globus recaps describe this as "the deployment of Globus Compute multi-user endpoints on SDSC's Expanse system", but SDSC's own slides frame it as in-progress/early-user. No UUID, no SDSC docs page (sdsc.edu Expanse user guide has no Globus Compute section). Classify as **pilot**.

## 3. Facilities checked with **no** documented MEP

- **NERSC (Perlmutter):** docs.nersc.gov has Globus *transfer* only (https://docs.nersc.gov/services/globus/). GlobusWorld 2024 NERSC talk ("Globus Compute with IRI workflows", N. Tyler) is "Still just a working proof of concept". Globus' example-config page has a single-user Perlmutter template.
- **OLCF (Frontier):** docs.olcf.ornl.gov mentions Globus collections only; 2024 ORNL talk (T. Skluzacek) covers Globus Compute in the ACE testbed, no MEP.
- **TACC (Frontera/Stampede3/Vista/Lonestar6), PSC Bridges-2, TAMU FASTER/ACES, IU Jetstream2:** only single-user example configs (Globus docs "Example Configurations": Delta, Expanse, UChicago AI Cluster, Kubernetes, Polaris, Perlmutter, Frontera, Bridges-2, FASTER, Open Science Pool, Stampede3) and/or the ACCESS Confluence page; facility docs have transfer-only Globus pages.
- **ALCF Aurora:** explicitly "does not yet support MEPs".
- **University centers searched** (UChicago RCC Midway, NCAR Derecho/Casper, Stanford Sherlock, Michigan, Harvard FASRC, Princeton, Yale, Utah CHPC, Arizona, Illinois Campus Cluster, OSC): no MEP documentation found; Midway/Bebop/UChicago-AI appear only as single-user example configs in Globus docs.
- **International:** Rosalind Franklin Institute (UK) has a GlobusWorld 2026 talk on Globus as an "access and enforcement layer" — no MEP claim. No hits for CSCS, JSC, EPCC, Pawsey, NCI, Alliance/Compute Canada.

## 4. Globus' official behaviour when identity mapping fails (and related)

Sources: MEP guide https://globus-compute.readthedocs.io/en/main/endpoints/multi_user.html ; troubleshooting https://globus-compute.readthedocs.io/en/main/endpoints/troubleshooting.html (source `docs/endpoints/troubleshooting.rst`) ; config reference https://globus-compute.readthedocs.io/en/main/endpoints/config_reference.html ; endpoint guide (auth policies) https://globus-compute.readthedocs.io/en/main/endpoints/endpoints.html#authentication-policies ; code `compute_endpoint/globus_compute_endpoint/endpoint/endpoint_manager.py` and `endpoint/utils/__init__.py` in https://github.com/globus/globus-compute.

- **Mapping failure is returned to the submitter, synchronously, as an API error (not logged-only).** Troubleshooting page, section *"Identity failed to map to a local user name" error*: "This happens when a multi-user endpoint doesn't recognize the submitting user identity, because the identity mapping configuration either is incorrect or intentionally excludes that identity." Example (edited for clarity in the docs):
  ```
  ComputeAPIError: ('POST', 'http://compute.api.globus.org/v3/endpoints/[ENDPOINT_ID]/submit', 'Bearer',
      422, 'SEMANTICALLY_INVALID',
      'Request payload failed validation: Identity failed to map to a local user name.  (LookupError)
         Globus effective identity: 3c085472-314d-4f22-abc6-591f2767af2b
         Globus username: alice@example.edu')
  ```
  "**Users**: contact the administrator of the endpoint in question; if you should have access and see this error, the endpoint is likely misconfigured." Admins get the `globus-idm-validator` tool and log lines containing "Globus effective identity" ("emitted whether or not the identity mapping succeeded").
- MEP guide wording: "if no username field in any of the identities in the set ended with `@example.com`, then it would not match and the start request would fail." Mapping is tried across the whole Globus *identity set* (linked identities), first match wins; "Multiple mappings may be defined, and each will be tried in order until one maps the identity successfully or no mappings are possible."
- **Mechanism (source):** `endpoint_manager.py` — on `LookupError` the MEP logs `"Identity failed to map to a local user name."` and calls `send_failure_notice(...)`, which forks a throw-away child that publishes an `EPStatusReport(global_state={"error": msg, ...})` on the would-be UEP's heartbeat queue ("This parameter will be presented to the user via the SDK as the reason for failure"); the web service surfaces it as the 422 above. Sibling messages: `"Identity mapped to a local user name, but local user does not exist."` (mapping matched but `getpwnam` failed); `"Ignoring start request for untrusted identity."` (MEP running unprivileged / no idmap); `"Unhandled error attempting to map to a local user name."`; default `"General or unknown failure starting user endpoint"`.
- **Other start-time failures reach the user the same way**, e.g. PAM account check: user's SDK sees `Request payload failed validation: Unable to start user endpoint process for jessica [exit code: 71; (PermissionError) see your system administrator]` while the endpoint log has `(error code: 7 [PAM_AUTH_ERR]) Authentication failure`. Startup-error exit codes 77 (`EX_NOPERM`, cannot read idmap file) and 78 (`EX_CONFIG`, cannot parse idmap file) are MEP-side/admin-facing.
- **"Mapping to a local user is required; if a multi-user endpoint does not have an identity mapping setup, it will fail to start."** (Globus blog, 2024-06-25.)
- **`public: true` is discoverability only:** "This field does not indicate access/usage of the endpoint. It determines only whether this endpoint is easily discoverable via the Globus web portal — access is controlled via the admins field (management of endpoint) and the identity mapping configuration (Compute usage)."
- **Authentication policies (the "session/consent-style" gate):** "Administrators can use a Globus authentication policy to limit access to a multi-user endpoint by enforcing that the user has appropriate identities linked to their Globus account and that the required identities have recent authentications." Configured via `authentication_policy: <uuid>` in `config.yaml` or `configure --allowed-domains/--excluded-domains/--auth-timeout`; "Users are evaluated against the policy when submitting tasks, retrieving endpoint information, etc." (changelog). `--auth-timeout` "will also set high_assurance to true"; HA needs an HA subscription; "the smallest timeout that Compute supports is 1 minute" (60 s web-service cache). The Compute SDK wraps client calls in a GARE handler (`compute_sdk/globus_compute_sdk/sdk/utils/gare.py`, `_client_gares_handler` in `client.py`): a policy failure comes back as a `GlobusAPIError` carrying `authorization_parameters` (e.g. `session_required_single_domain`, `session_required_policies`), and the SDK re-runs `app.login(auth_params=…)` — i.e. the user is pushed through a browser re-authentication, not a silent failure. Globus Auth docs: "If no linked identity meets the domain constraints, the user will be prompted to authenticate with an identity that matches the domain constraints. If no identity meets the domain constraints, the authentication flow will fail."
- **Globus OIDC path:** a site can run a Globus OIDC server (needs a GCS endpoint), restrict the MEP with `--allowed-domains <OIDC-domain>`, and map `(.*)@<OIDC-domain>` → local user; PAM is only used for account/session phases, never authentication.
- **UEP identity:** the web service derives the UEP id from `(endpoint_uuid, Globus identity of the requester, user_endpoint_config hash, user runtime)`; changing any config key forces a new UEP.

## 5. Is there a Globus page listing MEP deployments?

No. Checked: Globus news 2024-05-07 "Globus Announces Multi-User Support for Globus Compute" ("Multi-user Globus Compute endpoints are currently installed at several institutions" — none named); Globus blog 2024-06-25 (none named); https://www.globus.org/compute (none); https://docs.globus.org/compute/ (redirect to API docs); the MEP guide (only ALCF Polaris as a *template example*); PEARC'24 paper (ALCF only as an auth-policy example; evaluation on Jetstream). The de-facto listings are the GlobusWorld programs: 2024 (NeSI "First Steps with … Multi-User Endpoints"; NERSC PoC; ORNL ACE), 2025 (Purdue Anvil "we deployed GCE on Anvil"; SDSC Expanse "progress"), 2026 (admin deep-dive on "how to set up Globus Compute multi-user endpoints in common environments such as Slurm clusters"; Rosalind Franklin Institute). Program URLs: https://www.globusworld.org/conf/2024.php, https://www.globusworld.org/conf/2025.php, https://www.globusworld.org/conf/program?cL=raleigh.

## 6. Registry-relevant observations

- Only **four** facilities publish a MEP UUID in their own docs today: ALCF (Polaris, Crux), NCSA (Delta), NeSI (Mahuika ×2). Purdue's Anvil UUID is public via slides and downstream tools but not via Purdue docs; Expanse has none.
- Identity domains: ALCF → ALCF identity (`alcf.anl.gov`, MobilePass+); NCSA Delta → unstated; NeSI → `iam.nesi.org.nz` linked identity (must be linked first); Anvil → ACCESS credentials per the talk abstract.
- Required keys differ per facility and are case-sensitive: `queue`+`account` (ALCF, PBS), `account`+`partition` (Delta, Slurm), `ACCOUNT_ID` (NeSI), `account`/`qos` (Anvil, seen in the wild). ALCF's schema has `additionalProperties: true`, so typos are silently ignored rather than rejected.
- Unmapped-identity handling is uniform across MEPs (it is Globus-side): a 422 `SEMANTICALLY_INVALID` "Identity failed to map to a local user name" at submit time, with the submitter's effective identity/username echoed back — cheap to detect and turn into an actionable message.
- Two of the four documented deployments (ALCF, NeSI) pin worker Python/parsl versions the client must match (ALCF: Python 3.13, `parsl==2026.02.23`; NeSI: Python 3.11, SDK 3.x); Anvil's MEP reportedly runs Python 3.13.
- ALCF's own guidance for a wedged endpoint is `rm ~/.globus_compute/*/daemon.pid` over SSH — zero-SSH is the happy path, not the recovery path.

## 7. All URLs used

- Globus Compute MEP guide: https://globus-compute.readthedocs.io/en/main/endpoints/multi_user.html
- Globus Compute troubleshooting: https://globus-compute.readthedocs.io/en/main/endpoints/troubleshooting.html
- Globus Compute config reference: https://globus-compute.readthedocs.io/en/main/endpoints/config_reference.html
- Globus Compute endpoint guide (auth policies / HA): https://globus-compute.readthedocs.io/en/main/endpoints/endpoints.html
- Globus Compute example configs (single-user): https://globus-compute.readthedocs.io/en/stable/endpoints/endpoint_examples.html
- Source: https://github.com/globus/globus-compute (endpoint_manager.py, endpoint/utils/__init__.py, compute_sdk/…/utils/gare.py, docs/changelog.rst)
- Globus news 2024-05-07: https://www.globus.org/news/globus-announces-multi-user-support-for-globus-compute
- Globus blog 2024-06-25: https://www.globus.org/blog/globus-compute-multi-user-endpoints
- PEARC'24 paper: https://knowledge.uchicago.edu/record/13157 (PDF: https://knowledge.uchicago.edu/records/wtfqj-13586/files/Enabling-Remote-Management-of-FaaS-Endpoints-with-Globus-Compute-Multi-User-Endpoints.pdf)
- ALCF docs: https://docs.alcf.anl.gov/services/globus-compute/ ; source https://github.com/argonne-lcf/user-guides/blob/main/docs/services/globus-compute.md ; https://github.com/argonne-lcf/alcf-globus-compute ; https://github.com/argonne-lcf/alcf-agent-skillset/blob/main/globus/globus-compute-multiuser-endpoints.md
- NCSA Delta: https://docs.ncsa.illinois.edu/systems/delta/en/latest/user_guide/running_jobs.html ; https://github.com/ncsa/puppet-profile_globus
- NeSI: https://docs.nesi.org.nz/Batch_Computing/Globus_Compute/ ; https://github.com/nesi/support-docs/blob/main/docs/Batch_Computing/Globus_Compute.md ; https://www.slideshare.net/slideshow/first-steps-with-globus-compute-multi-user-endpoints-27d3/269445525
- Purdue: GlobusWorld 2025 slides https://g-1f310f.ac84b1.a567.data.globus.org/globusworld_2025/public/p_691e4a0d-3b68-4539-a01c-27e7a4aa701a.pdf ; https://github.com/Garden-AI/groundhog ; https://github.com/junghawoo/globus_compute
- SDSC: https://g-1f310f.ac84b1.a567.data.globus.org/globusworld_2025/public/p_951f926f-171a-4ff8-ad45-76fde92aad63.pdf ; https://www.globus.org/news/globusworld-2025-highlights ; https://www.hpcwire.com/2025/06/30/report-from-globusworld-2025-going-beyond-data/
- ACCESS: https://access-ci.atlassian.net/wiki/spaces/ACCESSdocumentation/pages/552828929/Globus+Compute+on+ACCESS
- NERSC: https://docs.nersc.gov/services/globus/ ; https://www.slideshare.net/slideshow/globus-compute-with-integrated-research-infrastructure-iri-workflows-e2eb/269408370
- OLCF: https://docs.olcf.ornl.gov/systems/frontier_user_guide.html ; https://www.slideshare.net/slideshow/enhancing-research-orchestration-capabilities-at-ornl-pdf/269445472
- GlobusWorld programs: https://www.globusworld.org/conf/2024.php ; https://www.globusworld.org/conf/2025.php ; https://www.globusworld.org/conf/program?cL=raleigh
- Globus Auth policies: https://docs.globus.org/api/auth/developer-guide/ ; GAREs: https://docs.globus.org/guides/overviews/gares/

## hpc-bridge implications (maintainer notes, 2026-09-03)

- **Registry candidates:** ALCF Polaris + Crux (PBS; `queue` + `account` required), NCSA Delta (Slurm; `account` + `partition`), NeSI Mahuika (early access; `ACCOUNT_ID`; login-shape AND compute-shape MEPs — the same split we use), Purdue Anvil's MEP (live but undocumented; ACCESS identities). Each needs a maintainer or a facility user to canary it before ingest.
- **Model gap — per-facility template keys.** `MEPFacility.from_entry` builds one fixed `user_endpoint_config` shape (`partition`/`account`/`walltime`/`init_blocks`…). Real MEP templates disagree and are case-sensitive: `queue` (ALCF), `ACCOUNT_ID` (NeSI), `qos` (Anvil); ALCF's schema is `additionalProperties: true`, so a wrong key is silently ignored. The catalog entry needs a key map (or per-facility template names) before any of these can be added. Track under [[Endpoint reuse and MEP integration]].
- **No-account failure is a synchronous 422** (`SEMANTICALLY_INVALID`, `Identity failed to map to a local user name … Globus username: <who>`) at submit — not a task-level failure. hpc-bridge keeps the API error's message (not a slice of its repr) and turns it into a terminal `down` naming the identity from the error itself.
- **Auth policies are M2 territory.** A facility auth policy (ALCF-style domain/timeout) surfaces as a GARE the SDK answers by re-running its OWN login flow — inside the MCP process that is the command-line flow on the transport. Our `LoginFlow` must own that re-auth before an ALCF entry ships (deferred to V1.x with M2).
- ALCF's wedged-endpoint recovery is `rm ~/.globus_compute/*/daemon.pid` over SSH: zero-SSH is the happy path, not the recovery path.

## Validated live through hpc-bridge (2026-09-04)

The template-key gap is closed: `MEPFacility` now reads the facility's published `user_config_schema`, `endpoint_version`
and display name at attach (`load_template`), drops `user_endpoint_config` keys a strict schema rejects
(`sanitize_uec`; Anvil's schema is `additionalProperties: false` and would have refused `interface`/`compute`/
`max_workers_per_node`), and resolves `{gce_version}`/`{python_version}` in the entry's worker_init so the worker pool is
pinned to the FACILITY's endpoint version and this client's Python — both facility templates' defaults would install the
worker at the client's SDK version instead (the globus1 skew failure). Entry `defaults.extra` passes the template's own
knobs (qos, cores_per_node, scheduler_options…). Driver: `agentic/mep_live_check.py SEED ENTRY --account …`.

| facility | result | detail |
|---|---|---|
| **NCSA Delta** (`4a266c83…`, v4.15.0) | **PASS** | account `bgta-delta-gpu`, partition `gpuA40x4` + `#SBATCH --gpus-per-node=1`; block up in ~80 s; worker on gpub082 (py3.13.15, gce 4.15.0 pinned); `hostname; whoami` → `amcsweeneyellerm`; stop → draining. Facility stderr: "Task sandboxing will not work due to endpoint misconfiguration" (their template). Seed `ncsa-delta.yaml`. |
| **Purdue Anvil MEP** (`5aafb4c1…`, manager v4.12.0, UEP v4.16.0; registry id `anvil` since 2026-09-04 — the SSH entry was retired) | **PASS** (after `worker_version: client`) | `gusellerm@uchicago.edu` mapped to `x-amcsweeneyel` (UEP started by the facility); strict schema satisfied after the drop; `shared` never scheduled in 15 min; `debug` block up in ~160 s, worker a006 (py3.13.12, gce 4.16.0 pinned to the CLIENT version — the UEP runs the client's SDK version, not the manager's 4.12.0; pinning 4.12 failed `--logconf`). `cis250223-gpu` on gpu-debug was REJECTED by QOS (GPU partitions need a GPU request) — proof the account reaches Slurm. Seed `anvil-mep` in `anvil.yaml`, ingested 2026-09-04. |
| **SDSC Expanse** (SSH, TOTP at every login) | **PASS** (login shape) | the pre-auth handoff exercised for real: probe refused under BatchMode → NeedsPreauth → the user opened the ControlMaster with the plugin's command (one code) → probe over it proposed ib0/debug → bootstrap self-provisioned uv + gce 4.16.0 at py3.13 (system python 3.6; first install >120 s → the 900 s bootstrap timeout) → login shape up in ~55 s (login02) → teardown (delete had to wait for stop to land). Seed `sdsc-expanse.yaml` (mfa-otp), ingested 2026-09-04. No compute block run yet. |

**Re-prove on 0.1.18 (2026-10-06)**, `agentic/mep_live_check.py`, identity `gusellerm@uchicago.edu`:
- **globus-labs** (lab MEP, rebuilt 2026-10-01) — **PASS**: worker on spark2 as `glabs-gc` in ~80 s. The client is
  Python 3.13.12 and the workers 3.12.3: the SDK only warns, and the shell function crosses fine.
- **Anvil** — **FAIL, then PASS after a seed fix.** The block ran (job 21135790, a004) and the canary EXECUTED (its
  stdout is in the UEP's `tasks_working_dir`), but the interchange dropped the result: `Failed to read manager message
  … too many values to unpack (expected 2)`. The facility rebuilds the user endpoint's venv at every start and had
  pulled parsl **2026.10.05** (released the day before); our worker venv, built earlier and only re-checked for gce,
  still had parsl **2026.08.10**. `--upgrade` in the entry's worker_init makes the worker resolve the same latest
  dependencies (`worker_env.strategy: float`, 0.1.19); re-run: worker on a000 as `x-amcsweeneyel` in ~180 s. Lesson: on a MEP whose UEP env floats, the
  worker must float the same way (or pin parsl to the UEP's) — pinning gce alone is not enough.
- **Delta** — **FAIL, the same skew in the OTHER direction** (first misread as facility-side: the web service showed
  every task `waiting-for-ep`, but on Delta the user endpoint HAD started, run the canaries, and its blocks had
  registered workers). Delta's endpoint runs the MEP's own fixed install — gce 4.16.0, **parsl 2026.08.10**, py3.13.13
  (its log) — while a fresh worker venv picks the newest parsl (2026.10.05; also what `--upgrade` does). The workers ran
  the tasks (`Completed executor task`) but no result arrived, and because the tasks stayed queued, BOTH of the day's
  user endpoints kept relaunching 15-minute A40 blocks (`idle_heartbeats_hard` = 48 h; hpc-bridge has no cancel channel
  on a MEP, and `stop_endpoint` on a UEP id is `ENDPOINT_NOT_FOUND` to the user). ~1.7 GPU-h before it was stopped: a
  temporary `~/.config/uv/uv.toml` `constraint-dependencies = ["parsl==2026.8.10"]` on Delta pinned the worker venv, the
  queued tasks completed (21 success, 3 failed on cancelled blocks), and the endpoints released their blocks.
  Fix (0.1.19): Delta's entry pins `"parsl==2026.8.10"` in its install, `worker_env.strategy: pin`.
- **Expanse** — pending (needs the user's one-time code).
