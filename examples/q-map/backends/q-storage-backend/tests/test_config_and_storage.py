import json
import tempfile
import unittest
from pathlib import Path

from q_storage_backend.config import parse_token_users
from q_storage_backend.models import SaveMapRequest
from q_storage_backend.storage import MapStore


class QStorageConfigTests(unittest.TestCase):
    def test_parse_token_users_from_dict(self):
        raw = json.dumps(
            {
                "token-a": {"name": "Alice", "email": "alice@example.com", "country": "IT"},
                "token-b": {"name": "Bob"},
            }
        )
        token_users = parse_token_users(raw)
        self.assertEqual(set(token_users.keys()), {"token-a", "token-b"})
        self.assertEqual(token_users["token-a"].name, "Alice")
        self.assertEqual(token_users["token-a"].email, "alice@example.com")
        self.assertEqual(token_users["token-b"].email, "token-b@example.com")

    def test_parse_token_users_from_list(self):
        raw = json.dumps(
            [
                {"token": "t1", "name": "User One"},
                {"token": "t2", "email": "two@example.com"},
            ]
        )
        token_users = parse_token_users(raw)
        self.assertEqual(set(token_users.keys()), {"t1", "t2"})
        self.assertEqual(token_users["t1"].name, "User One")
        self.assertEqual(token_users["t2"].email, "two@example.com")


class QStorageMapStoreTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = MapStore(Path(self.tmp.name))

    def tearDown(self):
        self.tmp.cleanup()

    def test_create_update_list_and_get_map(self):
        payload = SaveMapRequest(
            title="My Map",
            description="desc",
            map={"datasets": [], "config": {}},
            format="keplergl",
        )
        created = self.store.create_map("user-a", payload)
        self.assertEqual(created.title, "My Map")

        listed = self.store.list_maps("user-a")
        self.assertEqual(len(listed), 1)
        self.assertEqual(listed[0].id, created.id)
        self.assertEqual(listed[0].privateMap, True)

        loaded = self.store.get_map("user-a", created.id)
        self.assertIsNotNone(loaded)
        self.assertEqual(loaded.id, created.id)
        self.assertEqual(loaded.map, {"datasets": [], "config": {}})

        updated = self.store.update_map(
            "user-a",
            created.id,
            SaveMapRequest(
                title="My Map v2",
                description="updated",
                map={"datasets": [{"id": "d1"}], "config": {"version": "v1"}},
                format="keplergl",
                isPublic=True,
            ),
        )
        self.assertIsNotNone(updated)
        self.assertEqual(updated.title, "My Map v2")
        self.assertTrue(updated.updatedAt >= created.updatedAt)
        self.assertTrue(updated.isPublic)

        relisted = self.store.list_maps("user-a")
        self.assertEqual(relisted[0].title, "My Map v2")
        self.assertEqual(relisted[0].privateMap, False)

    def test_delete_map(self):
        payload = SaveMapRequest(
            title="To delete",
            description="tmp",
            map={"datasets": [], "config": {}},
            format="keplergl",
        )
        created = self.store.create_map("user-a", payload)
        self.assertIsNotNone(self.store.get_map("user-a", created.id))

        deleted = self.store.delete_map("user-a", created.id)
        self.assertTrue(deleted)
        self.assertIsNone(self.store.get_map("user-a", created.id))

        missing = self.store.delete_map("user-a", "missing-id")
        self.assertFalse(missing)

    def test_user_isolation_blocks_cross_user_read_update_delete(self):
        payload = SaveMapRequest(
            title="Private map",
            description="owned by user-a",
            map={"datasets": [{"id": "d1"}], "config": {}},
            format="keplergl",
        )
        created = self.store.create_map("user-a", payload)

        # user-b cannot see user-a map in list/get
        self.assertEqual(self.store.list_maps("user-b"), [])
        self.assertIsNone(self.store.get_map("user-b", created.id))

        # user-b cannot update user-a map
        updated = self.store.update_map(
            "user-b",
            created.id,
            SaveMapRequest(
                title="hijacked",
                description="should not be applied",
                map={"datasets": [], "config": {"tampered": True}},
                format="keplergl",
            ),
        )
        self.assertIsNone(updated)
        owner_loaded = self.store.get_map("user-a", created.id)
        self.assertIsNotNone(owner_loaded)
        self.assertEqual(owner_loaded.title, "Private map")

        # user-b cannot delete user-a map
        deleted_by_other = self.store.delete_map("user-b", created.id)
        self.assertFalse(deleted_by_other)
        self.assertIsNotNone(self.store.get_map("user-a", created.id))


if __name__ == "__main__":
    unittest.main()
