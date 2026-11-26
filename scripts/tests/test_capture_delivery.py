"""Regression coverage for capture-to-query evidence validation."""
import copy
import sys
from pathlib import Path
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from verify_capture_delivery import compare


class CaptureDeliveryTests(unittest.TestCase):
    def setUp(self):
        self.source = {"event_id":"event-1", "session_id":"session-1", "response_id":"response-1",
            "agent":{"name":"codex"}, "llm":{"model":"company-model"}, "side":"response",
            "path":"/v1/responses", "token_usage":{"input_tokens":17,"output_tokens":3},
            "conversation":{"entries":[{"role":"assistant","text":"local fixture"}],"completeness":"complete"}}
        self.record = dict(copy.deepcopy(self.source), event_type="response",
            http={"path":"/v1/responses","headers":[{"name":"authorization","value":"<redacted>"}]})

    def test_delivery_preserves_model_usage_and_conversation(self):
        result = compare([self.source], [self.record])
        self.assertEqual(result["token_usage"], {"input_tokens":17,"output_tokens":3})
        self.assertEqual(result["agents"], {"codex":1})

    def test_missing_changed_and_unredacted_records_fail(self):
        with self.assertRaises(AssertionError):
            compare([self.source], [])
        for field in ("token_usage", "conversation", "llm"):
            record = dict(self.record, **{field: {}})
            with self.subTest(field=field), self.assertRaises(AssertionError):
                compare([self.source], [record])
        self.record["http"]["headers"][0]["value"] = "fixture-secret"
        with self.assertRaises(AssertionError):
            compare([self.source], [self.record])


if __name__ == "__main__":
    unittest.main()
