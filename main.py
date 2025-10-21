# Libraries
import time
import logging
import sys

from textwrap import dedent
from dotenv import load_dotenv
load_dotenv()

# Internal Functions
from include.database_calls import get_urls
from include.api_calls import calling_api

# Logger
main_file_logger = logging.getLogger("main_script")
main_file_logger.setLevel(logging.INFO)

# File for logs
main_file_handler = logging.FileHandler("api_calls.log")
main_file_handler.setLevel(logging.INFO)
format = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
main_file_handler.setFormatter(format)

main_file_logger.addHandler(main_file_handler)

# Cool Banner
def cool_banner():
    banner =dedent("""
    ::::::::::::::::::::::::::::::::::::::::::
    :::::::::::: Running Script ::::::::::::::
    ::::::::::::::::::::::::::::::::::::::::::
    """)
    for line in banner.split("\n"):
        print(line)

# Animation
def loading_animation():
    animation = ["[■□□□□□□□□□]", "[■■□□□□□□□□]", "[■■■□□□□□□□]", "[■■■■□□□□□□]", "[■■■■■□□□□□]", 
                 "[■■■■■■□□□□]", "[■■■■■■■□□□]", "[■■■■■■■■□□]", "[■■■■■■■■■□]", "[■■■■■■■■■■]"]
    
    for _ in range(3):
        for frame in animation:
            sys.stdout.write(f"\r{frame} Loading...")
            sys.stdout.flush()
            time.sleep(0.2)
    print("\n✅ Script Started!\n")

# Main Function
def main():

    cool_banner()
    loading_animation()

    main_file_logger.info("Starting process")
    urls = get_urls()

    if urls is not None and len(urls) > 0:
        try:
            for url in urls:

                hourly_limit, minute_limit, daily_limit = calling_api(url, 1)

                if daily_limit == 0:
                    main_file_logger.info("Daily API limit reached stopping execution")
                    break

                if minute_limit == 0:
                    main_file_logger.info("Minute API limit reached pausing...")
                    time.sleep(70)

                if hourly_limit == 1:
                    main_file_logger.info("Hourly API limit reached stopping execution")
                    break

        except Exception as e:
            main_file_logger.error(f'Main process error: {e}', exc_info=True)
            print(f"Error occured: {e}")

    main_file_logger.info(f'Completed processing ...')

if __name__ == "__main__":
    main()