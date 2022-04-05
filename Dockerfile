# The application requires a docker client and git
FROM docker:20.10.14-git

ENV PYTHONUNBUFFERED=1

RUN apk add --update --no-cache python3 &&\
    python3 -m ensurepip &&\
    pip3 install --no-cache --upgrade pip setuptools

COPY . /usr/src/waldur_slurm/

WORKDIR /usr/src/waldur_slurm/

RUN pip3 install -r requirements.txt --no-cache-dir

CMD [ "python3", "-m", "waldur_slurm.main" ]
