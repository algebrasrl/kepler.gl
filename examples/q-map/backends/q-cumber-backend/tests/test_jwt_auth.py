import base64
import hashlib
import hmac
import json
import tempfile
import time
import unittest
from pathlib import Path

from fastapi.testclient import TestClient

from q_cumber_backend.config import JwtAuthSettings, Settings
from q_cumber_backend.main import create_app


def _encode_segment(payload: dict) -> str:
    raw = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _mint_hs256_token(payload: dict, secret: str) -> str:
    header = {"alg": "HS256", "typ": "JWT"}
    encoded_header = _encode_segment(header)
    encoded_payload = _encode_segment(payload)
    signing_input = f"{encoded_header}.{encoded_payload}".encode("ascii")
    signature = hmac.new(secret.encode("utf-8"), signing_input, hashlib.sha256).digest()
    encoded_signature = base64.urlsafe_b64encode(signature).rstrip(b"=").decode("ascii")
    return f"{encoded_header}.{encoded_payload}.{encoded_signature}"


class QCumberJwtAuthTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        providers_dir = self.root / "providers" / "it"
        providers_dir.mkdir(parents=True, exist_ok=True)
        (providers_dir / "provider.json").write_text(
            json.dumps(
                {
                    "id": "local-assets-it",
                    "name": "Local Assets IT",
                    "locale": "it",
                    "datasets": [],
                }
            ),
            encoding="utf-8",
        )
        self.providers_root = self.root / "providers"
        self.data_dir = self.root / "data"
        self.data_dir.mkdir(parents=True, exist_ok=True)

    def tearDown(self):
        self.tmp.cleanup()

    def _client(self, *, jwt_auth: JwtAuthSettings, api_token: str = "") -> TestClient:
        settings = Settings(
            api_token=api_token,
            user_name="Q-cumber User",
            user_email="qcumber@example.com",
            data_dir=self.data_dir,
            providers_dir=self.providers_root,
            cors_origins=["http://localhost:8081"],
            ai_hints_cache_ttl_seconds=3600,
            postgis_dsn="",
            postgis_host="localhost",
            postgis_port=5432,
            postgis_db="qvt",
            postgis_user="qvt",
            postgis_password="qvt",
            jwt_auth=jwt_auth,
        )
        return TestClient(create_app(settings))

    def test_jwt_read_role_is_enforced(self):
        secret = "qcumber-jwt-1"
        client = self._client(
            jwt_auth=JwtAuthSettings(
                enabled=True,
                hs256_secrets=(secret,),
                allowed_issuers=("qmap-ux",),
                allowed_audiences=("q-map",),
                require_audience=True,
                read_roles=("qmap-reader",),
            )
        )
        allowed_token = _mint_hs256_token(
            {
                "sub": "reader-1",
                "iss": "qmap-ux",
                "aud": "q-map",
                "exp": int(time.time()) + 300,
                "realm_access": {"roles": ["qmap-reader"]},
                "name": "Reader One",
                "email": "reader1@example.com",
            },
            secret,
        )
        denied_token = _mint_hs256_token(
            {
                "sub": "viewer-1",
                "iss": "qmap-ux",
                "aud": "q-map",
                "exp": int(time.time()) + 300,
                "realm_access": {"roles": ["qmap-viewer"]},
            },
            secret,
        )

        ok = client.get("/me", headers={"Authorization": f"Bearer {allowed_token}"})
        self.assertEqual(ok.status_code, 200)
        self.assertEqual(ok.json().get("email"), "reader1@example.com")

        forbidden = client.get("/me", headers={"Authorization": f"Bearer {denied_token}"})
        self.assertEqual(forbidden.status_code, 403)

    def test_static_token_mode_is_unchanged_when_jwt_disabled(self):
        client = self._client(jwt_auth=JwtAuthSettings(enabled=False), api_token="legacy-token")

        missing = client.get("/me")
        self.assertEqual(missing.status_code, 401)

        invalid = client.get("/me", headers={"Authorization": "Bearer wrong-token"})
        self.assertEqual(invalid.status_code, 401)

        ok = client.get("/me", headers={"Authorization": "Bearer legacy-token"})
        self.assertEqual(ok.status_code, 200)
        self.assertEqual(ok.json().get("name"), "Q-cumber User")


if __name__ == "__main__":
    unittest.main()
