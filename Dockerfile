FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*


COPY requirements.txt .

RUN pip install --upgrade pip

RUN apt update && apt install -y curl jq

RUN pip install -r requirements.txt

CMD [ "bash" ]