from watch_queue import watch_queue


def main():
    """
    Processes the queue exactly once and then exits.
    """
    watch_queue()


if __name__ == '__main__':
    main()
