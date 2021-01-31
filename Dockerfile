FROM ubuntu

RUN apt-get update \
  && apt-get install -y \
    python3 \
    python3-pip \
    supervisor

RUN pip3 install \
    numpy \
    pandas \
    psycopg2-binary \
    schedule \
    # fbprophet prerequisites:
    pytz==2019.3 \
    convertdate==2.2.0 \
    lunarcalendar \
    holidays \
    tqdm \
    pystan

# fbprophet needs certain packages to be present before installing it
RUN pip3 install fbprophet

COPY src/ /opt/app/lib/prediction/

ENV DOCKER_ACTIVE=true

ENTRYPOINT ["python3", "/opt/app/lib/prediction/main_scheduler.py"]
