from unittest.mock import patch, MagicMock

import kafka_pipeline.producer as producer_module


@patch("kafka_pipeline.producer.KafkaProducer")
def test_stream_to_kafka(mock_kafka_producer_class):
    mock_producer_instance = MagicMock()
    mock_kafka_producer_class.return_value = mock_producer_instance

    fake_data = [
        {"timestamp": "2024-01-01", "ticker": "ABC", "price": 123.45},
        {"timestamp": "2024-01-02", "ticker": "XYZ", "price": 234.56},
    ]

    producer_module.stream_to_kafka(fake_data, topic="test-topic")

    assert mock_producer_instance.send.call_count == len(fake_data)
    mock_producer_instance.send.assert_any_call("test-topic", value=fake_data[0])
    mock_producer_instance.send.assert_any_call("test-topic", value=fake_data[1])
    mock_producer_instance.flush.assert_called_once()
