FROM python:3.7-buster

RUN curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python

ENV PATH="${PATH}:/root/.poetry/bin"

WORKDIR /drama
COPY poetry.lock pyproject.toml /drama/

RUN poetry config virtualenvs.create false \
  && poetry install --no-dev --no-interaction --no-ansi

COPY . /drama

CMD ["poetry", "run", "drama", "server"]