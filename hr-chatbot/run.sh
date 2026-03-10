docker build -f backend/Dockerfile -t databricks-agent .
docker run --env-file backend/.env -p 8000:8000 databricks-agent
