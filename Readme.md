README.md

markdown

# Currency Exchange Rate Streaming


## Problem Statement

The goal is to track the latest exchange rates for specific currency pairs, determine if they are "active" (the last rate received within the last 30 seconds), and compute the percentage change from the previous day. The project consists of two scheduled jobs: one runs every hour, and another runs every minute.

## Why Spark Structured Streaming?

Apache Spark Structured Streaming is chosen for its scalability, ease of use, and ability to handle large streams of data efficiently. It allows for real-time processing of streaming data with low latency, making it suitable for monitoring currency rates that can change frequently.

## Prerequisites

- **Python**: Ensure you have Python installed. You can download it from [python.org](https://www.python.org/downloads/).
- **Apache Spark**: Ensure Apache Spark is installed on your machine. You can download it from [spark.apache.org](https://spark.apache.org/downloads.html).

## Installing PySpark

To install PySpark, you can use pip. Run the following command:

bash
pip install pyspark

Setup Instructions

    Create a directory structure as follows:

currency_exchange_rate_streaming/
├── currency_exchange_rate_streamer.py                 # Shared utilities and the CurrencyExchangeRateStreamer class
├── hourly_job.py                   					# Script to process hourly updates for currency exchange rates
├── minutely_job.py                 					# Script to process minutely updates for currency exchange rates
├── job_config.properties           					# Configuration settings for both minutely and hourly job scripts
├── test_cur_exc_rate_streamer.py   					# Unit tests for CurrencyExchangeRateStreamer functionality
├── README.md                       					# Project documentation

Place your CSV file containing the streaming data in the hourly_rates_sample.csv and minutely_rates_sample.csv.

Run the hourly job using:

bash

spark-submit hourly_job.py

Run the minutely job using:

bash

spark-submit minutely_job.py 

### Testing

- The test file `test_rate_streamer.py` includes unit tests for the `CurrencyExchangeRateStreamer` class. It validates the streaming data processing, ensuring active currency rates are correctly processed with calculated percentage changes based on yesterday's rates. To run the tests, use `pytest` to verify the correctness of the rate streaming logic.
