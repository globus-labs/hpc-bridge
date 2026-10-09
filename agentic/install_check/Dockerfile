# A clean machine for the install-experience check: Debian, Node 22, uv, and the LATEST releases of the three non-Claude
# harnesses a hackathon participant might bring, each installed the way its own docs say, as an ordinary user. No model,
# no login, no credentials — the check proves the documented install gets hpc-bridge's tools in front of each harness.
#
#   docker build -t hpcb-install-check agentic/install_check
#   docker run --rm -e REF=main hpcb-install-check
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates curl git xz-utils procps jq libatomic1 \
    && curl -fsSL https://deb.nodesource.com/setup_22.x | bash - \
    && apt-get install -y --no-install-recommends nodejs \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --create-home --shell /bin/bash participant
USER participant
WORKDIR /home/participant
ENV HOME=/home/participant
ENV PATH=/home/participant/.local/bin:/home/participant/.npm-global/bin:$PATH

# uv (hpc-bridge's only prerequisite) — the official installer
RUN curl -LsSf https://astral.sh/uv/install.sh | sh

# Codex and Pi — npm, the way each documents it
ARG CODEX_VERSION=0.162.0
ARG PI_VERSION=1.1.0
RUN npm config set prefix "$HOME/.npm-global" \
    && npm install -g --no-audit --no-fund "@openai/codex@${CODEX_VERSION}" "@earendil-works/pi-coding-agent@${PI_VERSION}"

# Hermes — its official installer, non-interactive, at the release tag (it bootstraps its own Python)
ARG HERMES_REF=v0.21.6
RUN curl -fsSL https://hermes-agent.nousresearch.com/install.sh | bash -s -- --skip-setup --branch "${HERMES_REF}"

COPY --chown=participant check.sh mcp_probe.py /home/participant/
CMD ["bash", "/home/participant/check.sh"]
