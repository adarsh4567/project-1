

# ENV UV_HTTP_TIMEOUT=180
# ENV UV_CONCURRENCY=3

# # -- Adding local package . --
# ADD . /deps/agent
# # -- End of local package . --

# # -- Installing all local dependencies --
# RUN for dep in /deps/*; do \
#         echo "Installing $dep"; \
#         if [ -d "$dep" ]; then \
#             echo "Installing $dep"; \
#             (cd "$dep" && PYTHONDONTWRITEBYTECODE=1 uv pip install --system --no-cache-dir -c /api/constraints.txt .); \
#         fi; \
#     done
# # -- End of local dependencies install --
# ENV LANGSERVE_GRAPHS='{"ObserverAgent": "/deps/agent/observer/graph.py:graph"}'



# # -- Ensure user deps didn't inadvertently overwrite langgraph-api
# RUN mkdir -p /api/langgraph_api /api/langgraph_runtime /api/langgraph_license && touch /api/langgraph_api/__init__.py /api/langgraph_runtime/__init__.py /api/langgraph_license/__init__.py
# RUN PYTHONDONTWRITEBYTECODE=1 uv pip install --system --no-cache-dir --no-deps -e /api
# # -- End of ensuring user deps didn't inadvertently overwrite langgraph-api --
# # -- Removing build deps from the final image ~<:===~~~ --
# RUN pip uninstall -y pip setuptools wheel
# RUN rm -rf /usr/local/lib/python*/site-packages/pip* /usr/local/lib/python*/site-packages/setuptools* /usr/local/lib/python*/site-packages/wheel* && find /usr/local/bin -name "pip*" -delete || true
# RUN rm -rf /usr/lib/python*/site-packages/pip* /usr/lib/python*/site-packages/setuptools* /usr/lib/python*/site-packages/wheel* && find /usr/bin -name "pip*" -delete || true
# RUN uv pip uninstall --system pip setuptools wheel && rm /usr/bin/uv /usr/bin/uvx

# WORKDIR /deps/agent

# COPY .env /deps/agent/.env


FROM --platform=linux/arm64 langchain/langgraph-api:3.12

ENV UV_HTTP_TIMEOUT=180
ENV UV_CONCURRENCY=3

# 1. Setup the directory
WORKDIR /deps/agent

# 2. Copy ONLY dependency definitions first (to leverage Docker caching)
# Assuming you have a pyproject.toml or requirements.txt. 
# If you only have raw python files, skip to step 3.
COPY pyproject.toml uv.lock* /deps/agent/

# 3. Install dependencies 
# Note: This runs only if pyproject.toml changes, not when your code changes.
RUN PYTHONDONTWRITEBYTECODE=1 uv pip install --system --no-cache-dir -c /api/constraints.txt . || echo "No setup file found, skipping pre-install"

# 4. NOW Copy the rest of the source code
COPY . /deps/agent

# 5. Final install check (fast if dependencies are already installed)
RUN PYTHONDONTWRITEBYTECODE=1 uv pip install --system --no-cache-dir -c /api/constraints.txt .

ENV LANGSERVE_GRAPHS='{"ObserverAgent": "/deps/agent/observer/graph.py:graph"}'

# -- Ensure user deps didn't inadvertently overwrite langgraph-api --
RUN mkdir -p /api/langgraph_api /api/langgraph_runtime /api/langgraph_license && \
    touch /api/langgraph_api/__init__.py /api/langgraph_runtime/__init__.py /api/langgraph_license/__init__.py
RUN PYTHONDONTWRITEBYTECODE=1 uv pip install --system --no-cache-dir --no-deps -e /api

# -- Cleanup --
RUN pip uninstall -y pip setuptools wheel && \
    rm -rf /usr/local/lib/python*/site-packages/pip* && \
    uv pip uninstall --system pip setuptools wheel && \
    rm /usr/bin/uv /usr/bin/uvx

# Explicit copy of env if needed (ensure it is NOT in .dockerignore if you want this to work, 
# or use --env-file during docker run instead of baking it in)
COPY .env /deps/agent/.env