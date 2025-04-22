from unittest.mock import patch

import kafka_pipeline.producer


@patch("kafka_pipeline.producer.KafkaProducer")
def test_stream_to_kafka(mock_producer):
    mock_instance = mock_producer.return_value
    mock_instance.send.return_value = True

    kafka_pipeline.producer.stream_to_kafka([{"example": "data"}], topic="test")
    mock_instance.send.assert_called()
