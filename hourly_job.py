import configparser
from currency_exchange_rate_streamer import CurrencyExchangeRateStreamer

class HourlyJob:
    def __init__(self, properties_file):
        # Read configuration from properties file
        config = configparser.ConfigParser()
        config.read(properties_file)

        self.streaming_data_path = config["DEFAULT"]["input_data_path_hourly"]
        self.yesterday_rates_path = config["DEFAULT"]["yesterday_rates_path_hourly"]
        self.shuffle_partitions = int(config["DEFAULT"]["shuffle_partitions_hourly"])

        try:
            self.streamer = CurrencyExchangeRateStreamer(self.streaming_data_path, self.yesterday_rates_path)
            # Set shuffle partitions based on properties file
            self.streamer.spark.conf.set("spark.sql.shuffle.partitions", str(self.shuffle_partitions))
        except FileNotFoundError as e:
            print(f"Error initializing CurrencyExchangeRateStreamer: {e}")
            raise

    def run(self):
        """
        Execute the hourly job for processing currency exchange rates.
        """
        try:
            input_stream = self.streamer.read_streaming_data()
            active_rates = self.streamer.process_active_rates(input_stream)

            query = active_rates.writeStream \
                .outputMode("append") \
                .format("csv") \
                .option("path", self.streamer.yesterday_rates_path) \
                .option("header", "true") \
                .option("checkpointLocation", "path/to/checkpoint_hourly") \
                .start()

            # Start the streaming query
            query.awaitTermination()

        except FileNotFoundError as e:
            print(f"File Error: {e}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    # Load properties file
    properties_file = "job_config.properties"
    try:
        # Run the hourly job with properties file
        hourly_job = HourlyJob(properties_file)
        hourly_job.run()
    except FileNotFoundError as e:
        print(f"Initialization Error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred in the main function: {e}")




        # Sample Input Data
        # --------------------------------------
        # 288230383844090142,1708466371185,EURUSD,1.080790000000000
        # 288230383844090156,1708466371215,GBPUSD,1.262300000000000
        # 288230383844090178,1708466371262,AUDUSD,0.654900000000000
        # 288230383844090182,1708466371285,EURUSD,1.080790000000000
        # 288230383844090218,1708466371433,EURGBP,0.856190000000000
        # --------------------------------------

        # Sample Output Data
        # --------------------------------------
        # The following output is printed to the console:
        # ccy_couple,rate,change_percentage
        # "EUR/USD",1.08079,"N/A"
        # "GBP/USD",1.26230,"N/A"
        # "AUD/USD",0.65490,"N/A"
        # "EUR/GBP",0.85619,"N/A"
        # --------------------------------------