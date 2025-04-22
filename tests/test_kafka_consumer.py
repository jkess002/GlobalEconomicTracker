from unittest.mock import patch, MagicMock

from kafka_pipeline.consumer import consume_messages


@patch('kafka_pipeline.consumer.KafkaConsumer')
def test_consumer_receives_data(mock_kafka_consumer):
    mock_message = MagicMock()
    mock_message.value = {"ticker": "AAPL", "price": 150.5}

    mock_kafka_consumer.return_value.__iter__.return_value = [mock_message]

    with patch('global_economic_tracker.kafka_pipeline.consumer.process_message') as mock_process:
        consume_messages("test-topic")

        mock_process.assert_called_once_with({"ticker": "AAPL", "price": 150.5})
