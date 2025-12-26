FROM --platform=linux/arm64 langchain/langgraph-api:3.12

ENV UV_HTTP_TIMEOUT=180
ENV UV_CONCURRENCY=3

# 1. Setup the directory
WORKDIR /deps/langgraph-intro

# 2. Copy ONLY dependency definitions first (to leverage Docker caching)
# Assuming you have a pyproject.toml or requirements.txt. 
# If you only have raw python files, skip to step 3.
COPY pyproject.toml uv.lock* /deps/langgraph-intro/

# 3. Install dependencies 
# Note: This runs only if pyproject.toml changes, not when your code changes.
RUN PYTHONDONTWRITEBYTECODE=1 uv pip install --system --no-cache-dir -c /api/constraints.txt . || echo "No setup file found, skipping pre-install"

# 4. NOW Copy the rest of the source code
COPY . /deps/langgraph-intro

# 5. Final install check (fast if dependencies are already installed)
RUN PYTHONDONTWRITEBYTECODE=1 uv pip install --system --no-cache-dir -c /api/constraints.txt .

ENV LANGSERVE_GRAPHS='{"scout": "/deps/langgraph-intro/scout/graph.py:graph"}'

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
COPY .env /deps/langgraph-intro/.env