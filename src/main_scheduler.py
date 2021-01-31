import time

import schedule

from watch_queue import watch_queue


def main():
    """
    Setups up scheduled tasks and keeps on running until terminated.
    """
    schedule.every(5).seconds.do(watch_queue)

    # Loop so that the scheduled tasks keep on running
    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == '__main__':
    main()
