#!/usr/bin/env bash
set -euo pipefail

CERT_DIR="certs"
CERT_FILE="${CERT_DIR}/cert.pem"
KEY_FILE="${CERT_DIR}/key.pem"

mkdir -p "$CERT_DIR"

if [[ -f "$CERT_FILE" && -f "$KEY_FILE" ]]; then
    echo "Certificates already exist in ${CERT_DIR}/"
    exit 0
fi

echo "Generating self-signed certificates for QUIC testing..."

# В Git Bash (MINGW/MSYS) отключаем автоматическое преобразование путей
if [[ "$(uname -s)" == MINGW* ]] || [[ "$(uname -s)" == MSYS* ]]; then
    export MSYS_NO_PATHCONV=1
fi

openssl req -x509 -newkey rsa:2048 -keyout "$KEY_FILE" -out "$CERT_FILE" -days 365 -nodes \
    -subj "/CN=localhost" \
    -addext "subjectAltName = DNS:localhost, IP:127.0.0.1"

echo "Certificates generated:"
echo "  cert: ${CERT_FILE}"
echo "  key:  ${KEY_FILE}"