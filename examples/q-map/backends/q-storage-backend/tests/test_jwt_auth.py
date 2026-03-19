import base64
import hashlib
import hmac
import json
import tempfile
import time
import unittest
from pathlib import Path

from fastapi.testclient import TestClient

from q_storage_backend.config import JwtAuthSettings, Settings, UserProfile
from q_storage_backend.main import create_app


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


class QStorageJwtAuthTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.data_dir = Path(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def _client(self, jwt_auth: JwtAuthSettings, *, api_token: str = "") -> TestClient:
        settings = Settings(
            api_token=api_token,
            default_user=UserProfile(
                id="local-user",
                name="Local User",
                email="local@example.com",
                registered_at="",
                country="IT",
            ),
            token_users={},
            data_dir=self.data_dir,
            cors_origins=["http://localhost:8081"],
            port=3005,
            jwt_auth=jwt_auth,
        )
        return TestClient(create_app(settings))

    def test_jwt_read_role_allows_me_but_blocks_write_without_write_role(self):
        secret = "jwt-secret-1"
        client = self._client(
            JwtAuthSettings(
                enabled=True,
                hs256_secrets=(secret,),
                allowed_issuers=("qmap-ux",),
                allowed_audiences=("q-map",),
                require_audience=True,
                read_roles=("qmap-reader",),
                write_roles=("qmap-editor",),
            )
        )
        token = _mint_hs256_token(
            {
                "sub": "alice",
                "iss": "qmap-ux",
                "aud": "q-map",
                "exp": int(time.time()) + 300,
                "realm_access": {"roles": ["qmap-reader"]},
            },
            secret,
        )
        headers = {"Authorization": f"Bearer {token}"}

        me = client.get("/me", headers=headers)
        self.assertEqual(me.status_code, 200)
        self.assertEqual(me.json().get("id"), "alice")

        create_payload = {
            "title": "No write",
            "description": "",
            "map": {"datasets": [], "config": {}},
            "format": "keplergl",
        }
        create = client.post("/maps", json=create_payload, headers=headers)
        self.assertEqual(create.status_code, 403)

    def test_jwt_subject_partitions_user_maps(self):
        secret = "jwt-secret-2"
        client = self._client(
            JwtAuthSettings(
                enabled=True,
                hs256_secrets=(secret,),
                allowed_issuers=("qmap-ux",),
                allowed_audiences=("q-map",),
                require_audience=True,
                read_roles=("qmap-reader",),
                write_roles=("qmap-editor",),
            )
        )
        writer_token = _mint_hs256_token(
            {
                "sub": "user-a@example.com",
                "iss": "qmap-ux",
                "aud": "q-map",
                "exp": int(time.time()) + 300,
                "realm_access": {"roles": ["qmap-reader", "qmap-editor"]},
            },
            secret,
        )
        reader_token = _mint_hs256_token(
            {
                "sub": "user-b@example.com",
                "iss": "qmap-ux",
                "aud": "q-map",
                "exp": int(time.time()) + 300,
                "realm_access": {"roles": ["qmap-reader"]},
            },
            secret,
        )

        writer_headers = {"Authorization": f"Bearer {writer_token}"}
        reader_headers = {"Authorization": f"Bearer {reader_token}"}

        create = client.post(
            "/maps",
            json={
                "title": "Private map",
                "description": "",
                "map": {"datasets": [{"id": "d1"}], "config": {}},
                "format": "keplergl",
            },
            headers=writer_headers,
        )
        self.assertEqual(create.status_code, 200)

        writer_maps = client.get("/maps", headers=writer_headers)
        self.assertEqual(writer_maps.status_code, 200)
        self.assertEqual(len(writer_maps.json().get("items", [])), 1)

        reader_maps = client.get("/maps", headers=reader_headers)
        self.assertEqual(reader_maps.status_code, 200)
        self.assertEqual(reader_maps.json().get("items"), [])

    def test_jwt_allowed_subjects_reject_unlisted_subject(self):
        secret = "jwt-secret-3"
        client = self._client(
            JwtAuthSettings(
                enabled=True,
                hs256_secrets=(secret,),
                allowed_issuers=("qmap-ux",),
                allowed_audiences=("q-map",),
                require_audience=True,
                allowed_subjects=("allowed-user",),
            ),
            api_token="legacy-token",
        )
        token = _mint_hs256_token(
            {
                "sub": "forbidden-user",
                "iss": "qmap-ux",
                "aud": "q-map",
                "exp": int(time.time()) + 300,
            },
            secret,
        )

        denied = client.get("/me", headers={"Authorization": f"Bearer {token}"})
        self.assertEqual(denied.status_code, 401)
        self.assertIn("subject", str(denied.json().get("detail", "")).lower())

        legacy = client.get("/me", headers={"Authorization": "Bearer legacy-token"})
        self.assertEqual(legacy.status_code, 401)

    def test_action_locked_map_update_requires_qh_iframe_claim_and_delete_is_blocked(self):
        secret = "jwt-secret-action-lock"
        client = self._client(
            JwtAuthSettings(
                enabled=True,
                hs256_secrets=(secret,),
                allowed_issuers=("qmap-ux",),
                allowed_audiences=("q-map",),
                require_audience=True,
                read_roles=("qmap-reader",),
                write_roles=("qmap-editor",),
            )
        )
        writer_token = _mint_hs256_token(
            {
                "sub": "writer-user",
                "iss": "qmap-ux",
                "aud": "q-map",
                "exp": int(time.time()) + 300,
                "realm_access": {"roles": ["qmap-reader", "qmap-editor"]},
            },
            secret,
        )
        iframe_writer_token = _mint_hs256_token(
            {
                "sub": "writer-user",
                "iss": "qmap-ux",
                "aud": "q-map",
                "exp": int(time.time()) + 300,
                "realm_access": {"roles": ["qmap-reader", "qmap-editor"]},
                "qh_action_map_write": True,
            },
            secret,
        )
        writer_headers = {"Authorization": f"Bearer {writer_token}"}
        iframe_writer_headers = {"Authorization": f"Bearer {iframe_writer_token}"}

        create = client.post(
            "/maps",
            json={
                "title": "Action locked map",
                "description": "",
                "map": {"datasets": [{"id": "d1"}], "config": {}},
                "format": "keplergl",
                "metadata": {
                    "locked": True,
                    "lockType": "action",
                    "actionUuid": "action-123",
                    "lockSource": "q_hive",
                },
            },
            headers=iframe_writer_headers,
        )
        self.assertEqual(create.status_code, 200)
        map_id = str(create.json().get("id") or "")
        self.assertTrue(map_id)

        update_without_claim = client.put(
            f"/maps/{map_id}",
            json={
                "title": "Updated without claim",
                "description": "",
                "map": {"datasets": [{"id": "d2"}], "config": {}},
                "format": "keplergl",
            },
            headers=writer_headers,
        )
        self.assertEqual(update_without_claim.status_code, 403)

        update_with_claim = client.put(
            f"/maps/{map_id}",
            json={
                "title": "Updated with claim",
                "description": "",
                "map": {"datasets": [{"id": "d3"}], "config": {}},
                "format": "keplergl",
            },
            headers=iframe_writer_headers,
        )
        self.assertEqual(update_with_claim.status_code, 200)

        delete_with_claim = client.delete(f"/maps/{map_id}", headers=iframe_writer_headers)
        self.assertEqual(delete_with_claim.status_code, 403)


if __name__ == "__main__":
    unittest.main()
