FROM python:alpine

WORKDIR /usr/src/autodownload

RUN apk add --update --no-cache openssh rsync
RUN pip install --upgrade pip && \
    pip install --no-cache-dir paho-mqtt requests

COPY messenger.py downloader.py .

CMD ["python3", "messenger.py"]