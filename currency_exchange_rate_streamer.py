from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, window, round

class CurrencyExchangeRateStreamer:
    def __init__(self, streaming_data_path, yesterday_rates_path):
        self.streaming_data_path = streaming_data_path
        self.yesterday_rates_path = yesterday_rates_path

        # Initialize Spark session
        self.spark = SparkSession.builder \
            .appName("Currency Exchange Rate Stream") \
            .getOrCreate()

        # Check if the files exist
        if not os.path.exists(self.streaming_data_path):
            raise FileNotFoundError(f"Streaming data file not found: {self.streaming_data_path}")

        if not os.path.exists(self.yesterday_rates_path):
            raise FileNotFoundError(f"Yesterday's rates file not found: {self.yesterday_rates_path}")


    def read_streaming_data(self):
        """
        Read the streaming data from the specified CSV location.
        """
        try:
            input_stream = self.spark.readStream \
                .schema("event_id LONG, event_time LONG, ccy_couple STRING, rate DOUBLE") \
                .option("header", "true") \
                .csv(self.streaming_data_path)
            return input_stream
        except Exception as e:
            print(f"Error reading streaming data: {e}")

    def process_active_rates(self, input_stream):
        """
        Process the stream to calculate the latest active rate for each currency pair.
        """
        try:
            active_rates = input_stream \
                .withColumn("event_time", (col("event_time") / 1000).cast("timestamp")) \
                .groupBy(
                    window(col("event_time"), "1 hour"),  # Aggregating every hour
                    "ccy_couple"
                ) \
                .agg(
                    expr("last(rate) as rate"),
                    expr("last(event_time) as last_time")
                ) \
                .filter(expr("last_time >= current_timestamp() - interval 30 seconds"))

            # Load yesterday's rates
            yesterday_rates = self.spark.read.csv(self.yesterday_rates_path, header=True, inferSchema=True)
            yesterday_rates.createOrReplaceTempView("yesterday_rates")

            # SQL query to calculate the change for active rates
            result = active_rates.createOrReplaceTempView("active_rates")
            query_result = self.spark.sql("""
                SELECT 
                    ar.ccy_couple,
                    ar.rate,
                    CASE 
                        WHEN yr.rate IS NOT NULL THEN 
                            CONCAT(ROUND(((ar.rate - yr.rate) / yr.rate) * 100, 3), '%')
                        ELSE 
                            'N/A' 
                    END AS change_percentage
                FROM active_rates ar
                LEFT JOIN yesterday_rates yr ON ar.ccy_couple = yr.ccy_couple
            """)
            return query_result
        except Exception as e:
            print(f"Error processing active rates: {e}")