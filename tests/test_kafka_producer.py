import pytest
from unittest.mock import patch
from kafka.producer import stream_to_kafka

@patch("my_kafka_module.producer.send")
def test_produce_to_kafka(mock_send):
    mock_send.return_value = True
    result = stream_to_kafka({"ticker": "AAPL", "price": 150})
    assert result is True
    mock_send.assert_called_once()
