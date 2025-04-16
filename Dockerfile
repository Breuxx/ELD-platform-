FROM python:3.10-slim

WORKDIR /app

# Сначала копируем только зависимости для кэширования
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Копируем весь код
COPY . .

# Запускаем uvicorn, обращаясь к main:app
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]