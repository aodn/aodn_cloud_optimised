FROM python:3.12-slim

ENV UV_PROJECT_ENVIRONMENT=/app/.venv
ENV UV_LINK_MODE=copy
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV VIRTUAL_ENV=/app/.venv
ENV PATH="${VIRTUAL_ENV}/bin:$PATH"

# Install uv
COPY --from=ghcr.io/astral-sh/uv:0.13.0 /uv /uvx /usr/local/bin/

# git is needed for the xarray fork dependency
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

# Set the working directory
WORKDIR /app

COPY . /app

# Install dependencies
RUN uv sync --frozen --extra notebooks --extra tests --extra docs --extra dev

# Enable bash-completion and set bash as the default shell
RUN echo "alias ll='ls -la'" >> ~/.bashrc
## Run tests and verify the package build
#RUN uv run pytest && \
#uv build && \
#pip install dist/*.whl

# Configure the shell to automatically activate the venv
RUN echo "source ${VIRTUAL_ENV}/bin/activate" >> ~/.bashrc

# Logging message
COPY motd /etc/motd

# Set permissions on the motd file
RUN chmod 644 /etc/motd

# Ensure the MOTD is displayed on login
RUN echo "cat /etc/motd" >> /root/.bashrc

CMD ["/usr/bin/bash"]
