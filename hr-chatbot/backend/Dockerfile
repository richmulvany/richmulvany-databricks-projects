# Stage 1: Build React frontend
FROM node:20 AS frontend-build
WORKDIR /frontend

COPY frontend/package*.json ./
RUN npm install

COPY frontend/ ./
RUN npm run build


# Stage 2: FastAPI backend
FROM python:3.11-slim

WORKDIR /app

COPY backend/app ./app
COPY backend/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY --from=frontend-build /frontend/build ./frontend_build

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]