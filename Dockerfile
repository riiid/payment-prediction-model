FROM tiangolo/uvicorn-gunicorn-fastapi:python3.8

WORKDIR /app

RUN apt-get update && \
    apt-get install -y bash curl

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .
CMD python app/api.py
EXPOSE 5000
