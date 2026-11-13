# Copyright (c) 2026 wei
# SPDX-License-Identifier: EPL-2.0

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "core"))
import sparkplug_b as sparkplug
from sparkplug_b_pb2 import Payload


class MetricTimestampTest(unittest.TestCase):
    def test_omitted_timestamp_is_evaluated_for_each_call(self):
        payload = Payload()
        with patch.object(sparkplug.time, "time", side_effect=[1000.001, 1000.002]):
            first = sparkplug.addMetric(payload, "a", 1, sparkplug.MetricDataType.Double, 1.0)
            second = sparkplug.addMetric(payload, "b", 2, sparkplug.MetricDataType.Double, 2.0)
        self.assertEqual(first.timestamp, 1000001)
        self.assertEqual(second.timestamp, 1000002)

    def test_explicit_timestamp_including_zero_is_preserved(self):
        for timestamp in (0, 123456):
            with self.subTest(timestamp=timestamp):
                with patch.object(sparkplug.time, "time") as clock:
                    metric = sparkplug.addMetric(
                        Payload(), "a", 1, sparkplug.MetricDataType.Double, 1.0, timestamp
                    )
                    self.assertEqual(metric.timestamp, timestamp)
                    clock.assert_not_called()

    def test_historical_metric_uses_current_default_timestamp(self):
        with patch.object(sparkplug.time, "time", return_value=2000.0):
            metric = sparkplug.addHistoricalMetric(
                Payload(), "a", 1, sparkplug.MetricDataType.Double, 1.0
            )
        self.assertTrue(metric.is_historical)
        self.assertEqual(metric.timestamp, 2000000)


if __name__ == "__main__":
    unittest.main()
