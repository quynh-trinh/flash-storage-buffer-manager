import time


class Timing:
    def __init__(self, desc):
        self.__desc = desc
        self.__start = 0.0
        self.__end = 0.0

    def __enter__(self):
        print(f"Begin timing {self.__desc}")
        self.__start = time.time()
        return self

    def __exit__(self, type, value, traceback):
        self.__end = time.time()
        print(f"Stop timing {self.__desc}")
        print(f"{self.__desc} took {self.get_time():.6f} s")

    def get_time(self):
        return self.__end - self.__start