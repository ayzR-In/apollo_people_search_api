# Libraries
import time

from dotenv import load_dotenv
load_dotenv()

# Internal Functions
from include.database_calls import get_urls
from include.api_calls import calling_api

# Main Function
def main():
    urls = get_urls()
    if urls is not None and len(urls) > 0:
        try:
            for url in urls:
                print(f"Processing Url:{url}")
                hourly_limit, minute_limit = calling_api(url, 1)

                if minute_limit == 0:
                    print("Minute API limit reached pausing...")
                    time.sleep(70)
                
                if hourly_limit == 0:
                    print("Hourly API limit reached stopping execution")
                    break

                # logging.info(f'Completed processing ...')
        except Exception as e:
            # logging.error(f'Main process error: {e}')
            print(e)

if __name__ == "__main__":
    main()