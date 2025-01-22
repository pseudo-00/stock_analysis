import yfinance as yf
import pandas as pd
from datetime import datetime
import logging
import yaml
import time
import psycopg2
from psycopg2 import extras
import plotly.graph_objects as go

logging.basicConfig(level=logging.DEBUG)

class StockIngestion:
    def __init__(self, config_path: str = "config.yaml"):
        """Initialize the stock data ingestion system"""
        self.setup_logging()
        self.load_config(config_path)
        self.setup_database()
    
    def setup_logging(self):
        """Set up logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers= [
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def load_config(self, config_path: str):
        """Load configuration from YAML file"""
        try:
            with open(config_path, 'r') as file:
                self.config = yaml.safe_load(file)
            self.symbols = self.config['symbols']
            self.interval = self.config['interval']
            self.logger.info(f"Loaded configuration for symbols: {self.symbols}")
        except Exception as e:
            self.logger.error(f"Error loading configuration: {str(e)}")
            raise
    
    def setup_database(self):
        """Set up QuestDB connection and create tables if they don't exist"""
        try:
            self.conn = psycopg2.connect(
                host = 'localhost',
                port = 8812,
                user = 'admin',
                password = 'quest',
                database = 'qdb'
            )
            with self.conn.cursor() as cur:
                # Create table for stock data
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS stock_prices (
                            timestamp TIMESTAMP,
                            symbol SYMBOL,
                            open DOUBLE,
                            high DOUBLE,
                            low DOUBLE,
                            close DOUBLE,
                            volume LONG
                            ) timestamp(timestamp) PARTITION BY DAY;
                """)
            self.conn.commit()
            self.logger.info("Successfully set up database connection and tables")
        except Exception as e:
            self.logger.error(f"Database setup error: {str(e)}")
            raise
    
    def fetch_real_time_data(self):
        """Fetch real-time stock data for configurated symbols"""
        data_frames = []

        for symbol in self.symbols:
            try:
                ticker = yf.Ticker(symbol)
                # Get real-time data
                data = ticker.history(period = '1d', interval = self.interval)

                if not data.empty:
                    data['symbol'] = symbol
                    data_frames.append(data)
                    self.logger.info(f"Successfully fetched data for {symbol}")
                else:
                    self.logger.warning(f"No data received for {symbol}")
            
            except Exception as e:
                self.logger.error(f"Error fetching data for {symbol}: {str(e)}")
        
        if data_frames:
            return pd.concat(data_frames)
        return None
    
    def process_data(self, df):
        """Process the fetched data"""
        if df is not None:
            df = df.reset_index()

            #Create processed dataframe with required columns

            processed_df = pd.DataFrame({
                'timestamp' : pd.to_datetime(df['Datetime']).dt.strftime('%Y-%m-%d %H:%M:%S'),
                'symbol' : df['symbol'],
                'open': df['Open'].astype(float),
                'high' : df['High'].astype(float),
                'low' : df['Low'].astype(float),
                'close': df['Close'].astype(float),
                'volume': df['Volume'].astype(int)
            })

            return processed_df
        return None
    
    def save_to_questDB(self, df):
        """Save to QuestDB"""
        if df is None or df.empty:
            return 
        
        # Convert Dataframe columns to native Python types (e.g., int instead of numpy.int64)
         # Convert 'volume' in int
         # Ensure timestamp is in the correct format
        
        try:

            # Convert dataframe to tuples for batch insert
            data = [(row['timestamp'], 
                     row['symbol'],
                     float(row['open']),
                     float(row['high']),
                     float(row['low']),
                     float(row['close']),
                     int(row['volume'])
            ) for _,row in df.iterrows()]

            self.logger.debug(f"Data to be inserted: {data[:10]}...")
            #Batch insert using psycopg2
            with self.conn.cursor() as cur:
                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO stock_prices (timestamp, symbol, open, high, low, close, volume)
                    VALUES %s
                    """,
                    data,
                    template="(%s, %s, %s, %s, %s, %s, %s)"
                )
            self.conn.commit()
            self.logger.info(f"Successfully saved {len(df)} records to QuestDB")
        except Exception as e:
            self.logger.error(f"Error saving to database: {str(e)}")
            self.conn.rollback()
    '''
    def show_candlestick_chart(self, df):
        """Generate and display a candlestick chart"""
        if df is None or df.empty:
            self.logger.warning("No data available to display candlestick chart")
            return

        self.logger.debug(f"Data for candlestick chart: {df.head()}")

        fig = go.Figure()

        for symbol in df['symbol'].unique():
            symbol_df = df[df['symbol'] == symbol].sort_values(by='timestamp')
            symbol_df['timestamp'] = pd.to_datetime(symbol_df['timestamp'])
            self.logger.debug(f"Data for {symbol}: {symbol_df.head()}")
            fig.add_trace(go.Candlestick(
                x=symbol_df['timestamp'],
                open=symbol_df['open'],
                high=symbol_df['high'],
                low=symbol_df['low'],
                close=symbol_df['close'],
                name=symbol
            ))

        fig.update_layout(
            title='Stock Prices Candlestick Chart',
            xaxis_title='Timestamp',
            yaxis_title='Price',
            xaxis_rangeslider_visible=False
        )

        self.logger.info("Displaying candlestick chart")
        fig.show()
'''
    def run(self):
        """Main execution method"""
        self.logger.info("Starting data ingestion process")
        while True:
            try:
                raw_data = self.fetch_real_time_data()
                if raw_data is not None:
                    self.logger.debug(f"Raw data: {raw_data.head()}")
                else:
                    self.logger.warning("No raw data fetched")

                processed_data = self.process_data(raw_data)
                if processed_data is not None:
                    self.logger.debug(f"Processed data: {processed_data.head()}")
                else:
                    self.logger.warning("No processed data available")

                if processed_data is not None:
                    self.save_to_questDB(processed_data)
                    self.show_candlestick_chart(processed_data)  # Display the candlestick chart
                
                # wait for the next interval
                time.sleep(self.config.get('fetch_interval', 60))
            
            except Exception as e:
                self.logger.error(f"Error in main execution loop: {str(e)}")
                time.sleep(10) # wait before retrying

if __name__ == "__main__":
    ingestion = StockIngestion()
    ingestion.run()