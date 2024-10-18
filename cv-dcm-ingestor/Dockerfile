FROM --platform=linux/amd64 python:3.8
#Labels as key value pair
LABEL Maintainer="tracebloc"

RUN mkdir /app
WORKDIR /app
ADD . /app/
# RUN pip install -r requirements.txt
RUN --mount=type=cache,target=/root/.cache/pip pip install -r requirement.txt

CMD ["python", "/app/ingestor.py"]