from schwab_streamer_client import SchwabStreamerClient
import time
from options import OptionsManager

def stream_spy():
    """
    Simple function to stream SPY data from Schwab's streaming API.
    This will connect to the streaming service and subscribe to SPY data.
    """
    # Initialize the client
    client = SchwabStreamerClient(debug=True)
    
    # Set SPY as the only symbol to track
    client.tracked_symbols = ["SPY"]
    
    try:
        # Connect to the streaming service
        print("Connecting to Schwab streaming service...")
        client.connect()
        
        # Keep the script running
        print("Streaming SPY data. Press Ctrl+C to stop.")
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\nStopping SPY stream...")
    except Exception as e:
        print(f"Error occurred: {e}")
    finally:
        # Clean up
        if client:
            client.disconnect()
            print("Disconnected from streaming service")

if __name__ == "__main__":
    options_manager = OptionsManager()

    # Get expiration and symbols first
    expiration_date = options_manager.get_option_expiration("SPY", 5)
    option_symbols = options_manager.get_option_symbols("SPY", expiration_date)

    # Start streaming
    options_manager.stream_options("SPY", 5)

    # Later, stop streaming
    options_manager.stop_streaming() 