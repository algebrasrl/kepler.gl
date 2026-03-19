#!/bin/sh
set -eu

token="${QMAP_AUTH_RUNTIME_TOKEN:-}"
mapbox_token="${QMAP_MAPBOX_TOKEN:-${VITE_MAPBOX_TOKEN:-}}"
escaped_token="$(printf '%s' "$token" | sed 's/\\/\\\\/g; s/"/\\"/g')"
escaped_mapbox_token="$(printf '%s' "$mapbox_token" | sed 's/\\/\\\\/g; s/"/\\"/g')"

mkdir -p /usr/share/nginx/html
cat > /usr/share/nginx/html/qmap-runtime-config.js <<EOF
window.__QMAP_AUTH_TOKEN__ = "${escaped_token}";
window.__QMAP_MAPBOX_TOKEN__ = "${escaped_mapbox_token}";
EOF

exec nginx -g 'daemon off;'
