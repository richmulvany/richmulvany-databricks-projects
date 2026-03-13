# ----------------------------
# Stage 1 — Build React frontend
# ----------------------------
FROM node:20 AS frontend-build

WORKDIR /frontend

COPY frontend/package*.json ./
RUN npm ci

COPY frontend/ ./
RUN npm run build


# ----------------------------
# Stage 2 — Python dependencies
# ----------------------------
FROM python:3.11-slim AS python-deps

WORKDIR /install

COPY backend/requirements.txt .

RUN pip install --upgrade pip

# Install dependencies into a separate directory
RUN pip install \
    --no-cache-dir \
    --prefix=/install \
    -r requirements.txt


# ----------------------------
# Stage 3 — Final runtime image
# ----------------------------
FROM python:3.11-slim

WORKDIR /app

# Copy installed Python packages
COPY --from=python-deps /install /usr/local

# Copy backend code
COPY backend/app ./app

# Copy frontend build
COPY --from=frontend-build /frontend/build ./frontend_build

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]