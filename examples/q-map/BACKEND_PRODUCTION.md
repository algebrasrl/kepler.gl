# q-map Backends Production Guide

This guide covers production deployment for the cloud-facing q-map backends:

- `examples/q-map/backends/q-cumber-backend` (`Q-cumber`)
- `examples/q-map/backends/q-storage-backend` (`Q-storage`)

## 1) Prerequisites

- Linux server with Python 3.10+
- TLS-terminated reverse proxy (Nginx, Caddy, Traefik, or cloud LB)
- Dedicated non-root user (for example `qmap`)
- Persistent data volumes for q-storage map JSON files and q-cumber metadata/cache

## 2) Create runtime users/dirs

Example (adapt paths to your environment):

```bash
sudo useradd --system --create-home --shell /usr/sbin/nologin qmap
sudo mkdir -p /opt/qmap/q-cumber /opt/qmap/q-storage
sudo mkdir -p /var/lib/qmap/q-cumber /var/lib/qmap/q-storage
sudo chown -R qmap:qmap /opt/qmap /var/lib/qmap
```

## 3) App install (one-time or on release)

Install each backend into its own venv:

```bash
# Q-cumber
cd /opt/qmap/q-cumber
python -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e /path/to/repo/examples/q-map/backends/q-cumber-backend

# Q-storage
cd /opt/qmap/q-storage
python -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e /path/to/repo/examples/q-map/backends/q-storage-backend
```

## 4) Production env files

### `/etc/qmap/q-cumber.env`

```bash
QCUMBER_BACKEND_TOKEN=
QCUMBER_JWT_AUTH_ENABLED=true
QCUMBER_JWT_HS256_SECRETS=<hs256-secret>
QCUMBER_JWT_ALLOWED_ISSUERS=qmap-ux
QCUMBER_JWT_ALLOWED_AUDIENCES=q-map
QCUMBER_JWT_REQUIRE_AUDIENCE=true
QCUMBER_JWT_READ_ROLES=qmap-reader,qmap-editor,qmap-admin
QCUMBER_USER_NAME=Q-cumber Service
QCUMBER_USER_EMAIL=ops@example.com
QCUMBER_DATA_DIR=/var/lib/qmap/q-cumber
QCUMBER_PROVIDERS_DIR=/opt/qmap/q-cumber/provider-descriptors
QCUMBER_CORS_ORIGINS=https://maps.example.com
```

### `/etc/qmap/q-storage.env`

```bash
QSTORAGE_API_TOKEN=
QSTORAGE_JWT_AUTH_ENABLED=true
QSTORAGE_JWT_HS256_SECRETS=<hs256-secret>
QSTORAGE_JWT_ALLOWED_ISSUERS=qmap-ux
QSTORAGE_JWT_ALLOWED_AUDIENCES=q-map
QSTORAGE_JWT_REQUIRE_AUDIENCE=true
QSTORAGE_JWT_READ_ROLES=qmap-reader,qmap-editor,qmap-admin
QSTORAGE_JWT_WRITE_ROLES=qmap-editor,qmap-admin
QSTORAGE_USER_NAME=Q-storage Service
QSTORAGE_USER_EMAIL=ops@example.com
QSTORAGE_DATA_DIR=/var/lib/qmap/q-storage
QSTORAGE_CORS_ORIGINS=https://maps.example.com
```

## 5) systemd services

### `/etc/systemd/system/qmap-qcumber-cloud.service`

```ini
[Unit]
Description=q-map Q-cumber Backend
After=network.target

[Service]
Type=simple
User=qmap
Group=qmap
WorkingDirectory=/opt/qmap/q-cumber
EnvironmentFile=/etc/qmap/q-cumber.env
ExecStart=/opt/qmap/q-cumber/.venv/bin/uvicorn qmap_qcumber_cloud.main:app --host 127.0.0.1 --port 3001 --workers 2 --proxy-headers
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
```

### `/etc/systemd/system/qmap-qstorage-cloud.service`

```ini
[Unit]
Description=q-map Q-storage Backend
After=network.target

[Service]
Type=simple
User=qmap
Group=qmap
WorkingDirectory=/opt/qmap/q-storage
EnvironmentFile=/etc/qmap/q-storage.env
ExecStart=/opt/qmap/q-storage/.venv/bin/uvicorn q_storage_backend.main:app --host 127.0.0.1 --port 3005 --workers 2 --proxy-headers
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
```

Enable/start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now qmap-qcumber-cloud
sudo systemctl enable --now qmap-qstorage-cloud
```

## 6) Reverse proxy (single domain example)

Nginx sketch:

```nginx
server {
  listen 443 ssl;
  server_name maps-api.example.com;

  location /qcumber/ {
    proxy_pass http://127.0.0.1:3001/;
    proxy_set_header Host $host;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;
  }

  location /qstorage/ {
    proxy_pass http://127.0.0.1:3005/;
    proxy_set_header Host $host;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;
  }
}
```

Then configure q-map frontend env:

```bash
VITE_QCUMBER_CLOUD_API_BASE=https://maps-api.example.com/qcumber
VITE_QSTORAGE_CLOUD_API_BASE=https://maps-api.example.com/qstorage
```

## 7) Health checks and smoke test

```bash
curl -s http://127.0.0.1:3001/health
curl -s http://127.0.0.1:3005/health
```

Authenticated smoke test:

```bash
curl -H "Authorization: Bearer <token>" http://127.0.0.1:3001/providers/locales
curl -H "Authorization: Bearer <token>" http://127.0.0.1:3005/me
```

## 8) Operational checklist

- Rotate `*_API_TOKEN` periodically
- Restrict CORS origins to production frontend only
- Back up `/var/lib/qmap/*` on schedule
- Monitor logs:
  - `journalctl -u qmap-qcumber-cloud -f`
  - `journalctl -u qmap-qstorage-cloud -f`
- Use staged rollout:
  - deploy to staging
  - run smoke tests
  - promote to production

## References

- FastAPI deployment docs:
  - https://fastapi.tiangolo.com/deployment/manually/
- Uvicorn deployment docs:
  - https://www.uvicorn.org/deployment/
  - https://www.uvicorn.org/deployment/docker/
- Nginx proxy module docs:
  - https://nginx.org/en/docs/http/ngx_http_proxy_module.html
