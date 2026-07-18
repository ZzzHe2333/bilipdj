from __future__ import annotations

import unittest

from core import server


class NormalizeServerConfigTests(unittest.TestCase):
    def test_defaults_to_localhost(self) -> None:
        self.assertEqual(
            server.normalize_server_config({}),
            {"host": "127.0.0.1", "port": 9816, "lan_listen": False},
        )

    def test_explicit_lan_listening_uses_wildcard_host(self) -> None:
        self.assertEqual(
            server.normalize_server_config({"lan_listen": True, "port": 9000}),
            {"host": "0.0.0.0", "port": 9000, "lan_listen": True},
        )

    def test_legacy_wildcard_hosts_enable_lan_listening(self) -> None:
        for host in ("0.0.0.0", "::", "[::]"):
            with self.subTest(host=host):
                normalized = server.normalize_server_config({"host": host})
                self.assertTrue(normalized["lan_listen"])
                self.assertEqual(normalized["host"], "0.0.0.0")

    def test_explicit_false_overrides_legacy_wildcard_host(self) -> None:
        normalized = server.normalize_server_config(
            {"host": "0.0.0.0", "lan_listen": False}
        )
        self.assertFalse(normalized["lan_listen"])
        self.assertEqual(normalized["host"], "127.0.0.1")

    def test_invalid_port_falls_back_to_default(self) -> None:
        normalized = server.normalize_server_config({"port": "invalid"})
        self.assertEqual(normalized["port"], server.DEFAULT_PORT)


if __name__ == "__main__":
    unittest.main()
