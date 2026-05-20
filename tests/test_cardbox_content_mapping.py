import unittest
from types import SimpleNamespace

from cardbox.structures import JsonContent, TextContent

from CommonGround.infra.content import PostgresCardBoxService


class CardboxContentMappingTests(unittest.TestCase):
    def test_encode_decode_round_trip_for_supported_payloads(self) -> None:
        cases = (
            ({"message": "hello"}, JsonContent),
            ([1, "two", False], JsonContent),
            ("plain text", TextContent),
            (7, JsonContent),
            (True, JsonContent),
            (None, JsonContent),
        )
        for payload, expected_type in cases:
            with self.subTest(payload=payload):
                encoded = PostgresCardBoxService._encode_payload(payload)
                self.assertIsInstance(encoded, expected_type)
                self.assertEqual(PostgresCardBoxService._decode_card_payload(SimpleNamespace(content=encoded)), payload)

    def test_encode_payload_rejects_non_finite_float(self) -> None:
        with self.assertRaises(TypeError):
            PostgresCardBoxService._encode_payload(float("nan"))
