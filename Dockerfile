# The application requires a docker client and git
FROM docker:20.10.14-git

ENV PYTHONUNBUFFERED=1

RUN apk add --update --no-cache python3 &&\
    python3 -m ensurepip &&\
    pip3 install --no-cache --upgrade pip setuptools poetry

RUN ln -s /usr/bin/python3.9 /usr/bin/python

COPY . /usr/src/waldur_slurm/

WORKDIR /usr/src/waldur_slurm/

RUN poetry config virtualenvs.create false && poetry install --only main

CMD [ "python3", "-m", "waldur_slurm.main" ]
