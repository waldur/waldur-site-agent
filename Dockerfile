# The application requires a docker client and git
FROM python:3.12-alpine

ENV PYTHONUNBUFFERED=1

RUN pip install --no-cache --upgrade pip setuptools poetry

COPY . /usr/src/waldur_site_agent/

WORKDIR /usr/src/waldur_site_agent/

RUN poetry config virtualenvs.create false && poetry install --only main

CMD [ "python3", "-m", "waldur_site_agent.main" ]
