FROM node@sha256:d649c27dae7ba0137b3cef5dd75baa422c08dc3d9e3fc0c23dfb172dc3cc6436 AS web
WORKDIR /web
COPY frontend/package*.json ./
RUN npm ci
COPY frontend/ ./
RUN npm run build
FROM python@sha256:2c941e860699f878900b0edc2403613c234d4b32eda3cc9fa7036991a2a63c4a
WORKDIR /workspace/backend
COPY backend/requirements.lock ./
RUN pip install --no-cache-dir -r requirements.lock
COPY backend/ ./
COPY --from=web /web/dist /workspace/frontend/dist
COPY config /workspace/config
COPY assets/avatar.png /workspace/assets/avatar.png
ENV PYTHONDONTWRITEBYTECODE=1
EXPOSE 8780
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8780", "--workers", "1", "--no-access-log"]
