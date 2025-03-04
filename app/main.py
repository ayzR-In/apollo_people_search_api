# Libraries
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
                print(url)
                calling_api(url, 1)
                # logging.info(f'Completed processing ...')
        except Exception as e:
            # logging.error(f'Main process error: {e}')
            print(e)

if __name__ == "__main__":
    main()