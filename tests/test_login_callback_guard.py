from __future__ import annotations

import json
import types
import unittest
from unittest import mock

from core.login_callback_guard import patch_login_callback, validate_callback_url


class _Logger:
    def __init__(self) -> None:
        self.messages: list[str] = []

    def warning(self, message: str, *args) -> None:
        self.messages.append(message % args if args else message)


class _Response:
    status = 204

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False


class LoginCallbackGuardTests(unittest.TestCase):
    def test_callback_url_policy(self) -> None:
        self.assertEqual(
            validate_callback_url("https://example.com/hook"),
            "https://example.com/hook",
        )
        self.assertEqual(
            validate_callback_url("http://127.0.0.1:9000/hook"),
            "http://127.0.0.1:9000/hook",
        )
        self.assertEqual(
            validate_callback_url("http://[::1]:9000/hook"),
            "http://[::1]:9000/hook",
        )
        for value in (
            "http://example.com/hook",
            "ftp://example.com/hook",
            "https://user:password@example.com/hook",
            "not-a-url",
        ):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    validate_callback_url(value)

    def _build_module(self):
        module = types.ModuleType("callback_server")

        def insecure_callback(*_args, **_kwargs):
            return True, "insecure"

        module._dispatch_login_callback = insecure_callback
        self.assertTrue(patch_login_callback(module))
        return module

    def test_external_plain_http_is_rejected_before_network_access(self) -> None:
        module = self._build_module()
        logger = _Logger()
        with mock.patch("core.login_callback_guard.urllib.request.build_opener") as opener:
            ok, message = module._dispatch_login_callback(
                {"enabled": True, "url": "http://example.com/hook"},
                cookie="SESSDATA=secret",
                bilibili_data={"uid": 1},
                logger=logger,
            )
        self.assertFalse(ok)
        self.assertIn("rejected", message)
        opener.assert_not_called()

    def test_https_callback_sends_json_and_caps_timeout(self) -> None:
        module = self._build_module()
        logger = _Logger()
        opener = mock.Mock()
        opener.open.return_value = _Response()
        with mock.patch(
            "core.login_callback_guard.urllib.request.build_opener",
            return_value=opener,
        ):
            ok, message = module._dispatch_login_callback(
                {
                    "enabled": True,
                    "url": "https://example.com/hook",
                    "auth_token": "token",
                    "timeout_seconds": 999,
                },
                cookie="SESSDATA=secret",
                bilibili_data={"uid": 1},
                logger=logger,
            )
        self.assertTrue(ok)
        self.assertIn("204", message)
        request = opener.open.call_args.args[0]
        self.assertEqual(opener.open.call_args.kwargs["timeout"], 30)
        payload = json.loads(request.data.decode("utf-8"))
        self.assertEqual(payload["cookie"], "SESSDATA=secret")
        self.assertEqual(payload["bilibili"]["uid"], 1)
        self.assertEqual(request.get_header("Authorization"), "Bearer token")

    def test_disabled_callback_does_not_validate_or_send(self) -> None:
        module = self._build_module()
        logger = _Logger()
        with mock.patch("core.login_callback_guard.urllib.request.build_opener") as opener:
            result = module._dispatch_login_callback(
                {"enabled": False, "url": "http://example.com/hook"},
                cookie="secret",
                bilibili_data={},
                logger=logger,
            )
        self.assertEqual(result, (False, "callback disabled"))
        opener.assert_not_called()


if __name__ == "__main__":
    unittest.main()
