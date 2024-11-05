FROM python:3.12-slim

# Set environment variables for Poetry
ENV POETRY_VERSION=1.8.4
ENV POETRY_HOME=/opt/poetry
ENV POETRY_NO_INTERACTION=1
ENV POETRY_VIRTUALENVS_IN_PROJECT=1
ENV POETRY_VIRTUALENVS_CREATE=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV VIRTUAL_ENV=/app/.venv
ENV PATH="${VIRTUAL_ENV}/bin:$PATH"

# Install Poetry and dependencies
RUN apt-get update && apt-get install -y curl && \
    curl -sSL https://install.python-poetry.org | python3 - && \
    ln -s ${POETRY_HOME}/bin/poetry /usr/local/bin/poetry

# Set the working directory
WORKDIR /app

COPY . /app
#COPY poetry.lock pyproject.toml /app/

# Install dependencies
RUN poetry install --with dev --no-interaction # --no-root


# Enable bash-completion and set bash as the default shell
RUN echo "alias ll='ls -la'" >> ~/.bashrc && \
    echo "alias poetry='poetry run'" >> ~/.bashrc
## Run tests and verify the package build
#RUN poetry run pytest && \
    #poetry build && \
    #pip install dist/*.whl

# Configure the shell to automatically activate the venv
RUN echo "source ${VIRTUAL_ENV}/bin/activate" >> ~/.bashrc

#ENTRYPOINT ["bash"]

CMD ["/usr/bin/bash"]
