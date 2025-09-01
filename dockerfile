FROM bitnami/airflow:latest

# Set working directory first
WORKDIR /opt/bitnami/airflow/dags

# Install uv as root (bitnami images typically run as root initially)
USER root
RUN pip install uv

# Copy dependency files first for better layer caching
COPY pyproject.toml uv.lock ./

# Install dependencies
RUN uv sync --frozen

# Copy DAGs and other application files
COPY --chown=1001:1001 . .

# Switch to the airflow user (bitnami uses UID 1001)
USER 1001