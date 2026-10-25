from dotenv import load_dotenv

from producer import create_producer
from yahoo_finance import fetch_and_publish as fetch_yahoo
from fred import fetch_and_publish as fetch_fred
from eia import fetch_and_publish as fetch_eia


def main():
    load_dotenv()

    producer = create_producer()
    total = 0

    try:
        for fetch in (fetch_yahoo, fetch_fred, fetch_eia):
            try:
                total += fetch(producer)
            except Exception as exc:
                print(f"Data source failed: {exc}")

        producer.flush()
        print(f"Total observations published: {total}")

    finally:
        producer.close()


if __name__ == "__main__":
    main()
