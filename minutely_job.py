import configparser
from currency_exchange_rate_streamer import CurrencyExchangeRateStreamer

class MinutelyJob:
    def __init__(self, properties_file):
        # Read configuration from properties file
        config = configparser.ConfigParser()
        config.read(properties_file)

        self.streaming_data_path = config["DEFAULT"]["input_data_path_minutely"]
        self.yesterday_rates_path = config["DEFAULT"]["yesterday_rates_path_minutely"]
        self.shuffle_partitions = int(config["DEFAULT"]["shuffle_partitions_minutely"])

        try:
            self.streamer = CurrencyExchangeRateStreamer(self.streaming_data_path, self.yesterday_rates_path)
            # Set shuffle partitions based on properties file
            self.streamer.spark.conf.set("spark.sql.shuffle.partitions", str(self.shuffle_partitions))
        except FileNotFoundError as e:
            print(f"Error initializing CurrencyExchangeRateStreamer: {e}")
            raise

    def run(self):
        """
        Execute the minutely job for processing currency exchange rates.
        """
        try:
            input_stream = self.streamer.read_streaming_data()
            active_rates = self.streamer.process_active_rates(input_stream)

            query = active_rates.writeStream \
                .outputMode("append") \
                .format("csv") \
                .option("path", self.streamer.yesterday_rates_path) \
                .option("header", "true") \
                .option("checkpointLocation", "path/to/checkpoint_minute") \
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
        # Run the minutely job with properties file
        minutely_job = MinutelyJob(properties_file)
        minutely_job.run()
    except FileNotFoundError as e:
        print(f"Initialization Error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred in the main function: {e}")
        
        
        
        
        
        # Sample Input Data
        # --------------------------------------
        # 288230383844090182,1708466371285,EURUSD,1.080790000000000
        # 288230383844090230,1708466371533,EURGBP,0.856190000000000
        # 288230383844090290,1708466371784,EURUSD,1.080800000000000
        # 288230383844093200,1708466384333,EURUSD,1.080740000000000
        # --------------------------------------

        # Sample Output Data
        # --------------------------------------
        # The following output is printed to the console:
        # ccy_couple,rate,change_percentage
        # "EUR/USD",1.08079,"0.000%"
        # "EUR/GBP",0.85619,"N/A"
        # --------------------------------------
