FROM python:3.12-slim

# Set environment variables for Poetry
ENV POETRY_VERSION=1.6.1 \
    POETRY_VIRTUALENVS_CREATE=true \
    POETRY_VIRTUALENVS_IN_PROJECT=true \
    POETRY_HOME="/opt/poetry"

# Install Poetry and dependencies
RUN apt-get update && apt-get install -y curl && \
    curl -sSL https://install.python-poetry.org | python3 - && \
    ln -s ${POETRY_HOME}/bin/poetry /usr/local/bin/poetry

# Set the working directory
WORKDIR /app

COPY poetry.lock pyproject.toml /app/

# Install dependencies
RUN poetry install --no-interaction --no-root

COPY . /app

# Install the project
RUN poetry install --no-interaction

# Run tests and verify the package build
RUN poetry run pytest && \
    poetry build && \
    pip install dist/*.whl
