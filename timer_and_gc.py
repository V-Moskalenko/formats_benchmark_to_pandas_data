import gc
import time


class Timer:
    """Таймер измерения времени выполнения функции, в качестве контекстного менеджера
        >>> import time
        >>> with Timer() as timer:
        ...     time.sleep(1)
        >>> print(float(timer))
        1.003992499987362
        """

    def __init__(self):
        self._start_time = None
        self.elapsed_time = None

    def __enter__(self):
        """Запустить новый таймер"""
        self._start_time = time.perf_counter()
        return self

    def __exit__(self, *exc_info):
        """Остановить таймер"""
        self.elapsed_time = time.perf_counter() - self._start_time

    def __float__(self):
        """Получить данные в формате числа с плавающей точкой"""
        return self.elapsed_time


class GC:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, ext_tb):
        self.collected = gc.collect()
