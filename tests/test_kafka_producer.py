from unittest.mock import MagicMock, patch

from kafka_pipeline import producer


@patch("kafka_pipeline.producer.KafkaProducer")
def test_stream_to_kafka(mock_producer_class):
    mock_producer = MagicMock()
    mock_producer_class.return_value = mock_producer

    sample_data = [{"ticker": "TEST", "price": 100, "timestamp": "2025-04-22"}]
    producer.stream_to_kafka(sample_data)

    mock_producer.send.assert_called()
