FROM python:3.10.0

WORKDIR /app

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY . .

CMD ["uvicorn", "src.main:app", "--reload", "--host", "0.0.0.0" "--port", "8001"]