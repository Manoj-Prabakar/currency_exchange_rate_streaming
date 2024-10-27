import pytest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from currency_exchange_rate_streamer import CurrencyExchangeRateStreamer
from pyspark.sql.functions import col

@pytest.fixture(scope="module")
def spark():
    # Initialize a Spark session
    spark = SparkSession.builder \
        .appName("Currency Exchange Rate Stream Test") \
        .master("local[2]") \
        .getOrCreate()
    yield spark
    spark.stop()

@pytest.fixture
def streamer(spark, tmp_path):
    # Define file paths for the test
    streaming_data_path = tmp_path / "rates_sample.csv"
    yesterday_rates_path = tmp_path / "yesterday_rates.csv"

    # Sample data simulating the rates_sample.csv
    streaming_data = [
        Row(event_id=288230383844090142, event_time=1708466371185, ccy_couple="EURUSD", rate=1.08079),
        Row(event_id=288230383844090156, event_time=1708466371215, ccy_couple="GBPUSD", rate=1.26230),
        Row(event_id=288230383844090178, event_time=1708466371262, ccy_couple="AUDUSD", rate=0.65490),
        Row(event_id=288230383844090182, event_time=1708466371285, ccy_couple="EURUSD", rate=1.08079),
        Row(event_id=288230383844090218, event_time=1708466371433, ccy_couple="EURGBP", rate=0.85619),
        Row(event_id=288230383844090290, event_time=1708466371784, ccy_couple="EURUSD", rate=1.08080)
    ]
    yesterday_data = [
        Row(ccy_couple="EURUSD", rate=1.08070),
        Row(ccy_couple="GBPUSD", rate=1.26220),
        Row(ccy_couple="AUDUSD", rate=0.65480),
        Row(ccy_couple="EURGBP", rate=0.85600)
    ]

    # Write sample data to CSV for testing
    spark.createDataFrame(streaming_data).write.csv(str(streaming_data_path), header=True, mode="overwrite")
    spark.createDataFrame(yesterday_data).write.csv(str(yesterday_rates_path), header=True, mode="overwrite")

    # Initialize the CurrencyExchangeRateStreamer with the test data paths
    streamer = CurrencyExchangeRateStreamer(str(streaming_data_path), str(yesterday_rates_path))
    return streamer

def test_process_active_rates(streamer, spark):
    # Read streaming data
    input_stream = streamer.read_streaming_data()

    # Process active rates
    active_rates = streamer.process_active_rates(input_stream)

    # Collect result
    result = active_rates.collect()

    # Expected output
    expected_data = [
        {"ccy_couple": "EURUSD", "rate": 1.08080, "change_percentage": "0.009%"},
        {"ccy_couple": "GBPUSD", "rate": 1.26230, "change_percentage": "0.008%"},
        {"ccy_couple": "AUDUSD", "rate": 0.65490, "change_percentage": "0.015%"},
        {"ccy_couple": "EURGBP", "rate": 0.85619, "change_percentage": "0.022%"}
    ]

    # Convert result to dictionary for comparison
    result_data = [{row["ccy_couple"]: row["rate"], row["change_percentage"]} for row in result]

    # Assert the result matches the expected data
    assert result_data == expected_data
