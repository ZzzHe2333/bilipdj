from __future__ import annotations

import datetime as dt
import logging
import tempfile
import types
import unittest
from pathlib import Path

from core.log_manager import DailyCategoryFileHandler, _configured_room_token


class DailyLogRotationTests(unittest.TestCase):
    def test_handler_switches_to_current_day_after_midnight(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            handler = DailyCategoryFileHandler("common", "123", Path(temp_dir))
            try:
                handler._current_day = dt.date.today() - dt.timedelta(days=1)
                record = logging.LogRecord(
                    name="test",
                    level=logging.INFO,
                    pathname=__file__,
                    lineno=1,
                    msg="hello",
                    args=(),
                    exc_info=None,
                )
                handler.emit(record)
                self.assertIn(dt.datetime.now().strftime("%Y%m%d"), Path(handler.baseFilename).name)
                self.assertTrue(Path(handler.baseFilename).is_file())
            finally:
                handler.close()

    def test_missing_config_file_uses_unknow_even_with_example_room(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            module = types.SimpleNamespace(CONFIG_PATH=Path(temp_dir) / "config.yaml")
            token = _configured_room_token(
                module,
                {"platform": "bilibili", "bilibili": {"roomid": 3049445}},
            )
            self.assertEqual(token, "unknow")


if __name__ == "__main__":
    unittest.main()
