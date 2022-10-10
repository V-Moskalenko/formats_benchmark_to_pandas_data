import os
import numpy
import pandas
import psutil
from uuid import uuid4
from timer_and_gc import Timer, GC
from multiprocessing import Process, Manager


def process_memory():
    """Функция для получения резидентного размера набора использования оперативной памяти (RSS)"""
    process = psutil.Process(os.getpid())
    mem_info = process.memory_info()
    return mem_info.rss


# Функция - декоратор
def profile(func):
    """Функция для получения изменения оперативной памяти во время выполнения функции"""

    def wrapper(*args, **kwargs):
        memory_before = process_memory()
        result = func(*args, **kwargs)
        memory_after = process_memory()
        ram_delta_mb = (memory_after - memory_before) / (1024 ** 2)
        return result, ram_delta_mb

    return wrapper


def generate_data_frame(count_rows: int, count_columns_number: int, count_columns_string: int, as_category=False):
    """
    Функция для генерации Data Frame.
    Args:
        count_rows: Количество строк
        count_columns_number: Количество столбцов с цифрами
        count_columns_string: Количество столбцов со стоками
        as_category: Если True, то тип данных "object" будет конвектирован в тип данных "category"

    Returns: pandas.core.frame.DataFrame
    """
    data = {}

    for column in range(count_columns_number):
        header = f'Число №{column}'
        values = numpy.random.normal(0, 1, count_rows)
        data[header] = values

    for column in range(count_columns_string):
        header = f'Строка №{column}'
        values = [str(uuid4()) for _ in range(count_rows)]
        data[header] = values

    data_frame = pandas.DataFrame(data)

    if as_category:
        object_columns = data_frame.select_dtypes(include=object).columns
        data_frame[object_columns] = data_frame[object_columns].astype('category')

    return data_frame


@profile
def save_data_frame(data_frame, format: str, filename: str) -> None:
    """
    Функция для сохранения Data Frame в необходимый формат.
    Выполняется под декоратором profile, для получения изменения RAM.
    P.S. Пробовал через getattr(data_frame, f'to_{format}'), но значения потребления памяти были некорректными
    Args:
        data_frame: Data Frame
        format: Формат, в который необходимо перевести Data Frame
        filename: Имя конечно файла, например "file_benchmark.feather"

    Returns: None

    """
    path = rf'test_formats\{filename}'
    if format == 'hdf':
        data_frame.to_hdf(path, key='df', format='table')
    elif format == 'csv':
        data_frame.to_csv(path)
    elif format == 'json':
        data_frame.to_json(path)
    elif format == 'pickle':
        data_frame.to_pickle(path)
    elif format == 'feather':
        data_frame.to_feather(path)
    elif format == 'parquet':
        data_frame.to_parquet(path)
    elif format == 'excel':
        data_frame.to_excel(path)


@profile
def load_data_frame(format: str, filename: str) -> None:
    """
    Функция для загрузки Data Frame из файла.
    Выполняется под декоратором profile, для получения изменения RAM.
    P.S. Пробовал через getattr(pandas, f'read_{format}'), но значения потребления памяти были некорректными
    Args:
        format: Формат файла, который необходимо прочитать
        filename: Имя файла, например "file_benchmark.feather"

    Returns: None

    """
    path = rf'test_formats\{filename}'
    if format == 'hdf':
        pandas.read_hdf(path)
    elif format == 'csv':
        pandas.read_csv(path)
    elif format == 'json':
        pandas.read_json(path)
    elif format == 'pickle':
        pandas.read_pickle(path)
    elif format == 'feather':
        pandas.read_feather(path)
    elif format == 'parquet':
        pandas.read_parquet(path)
    elif format == 'excel':
        pandas.read_excel(path)


def process_save_data(format: str, data_frame, filename: str, result: dict) -> None:
    """
    Функция, для того чтобы запускать save_data_frame отдельным процессом - для корректного получения значений
    Args:
        format: Формат файла
        data_frame: Data Frame
        filename: Имя файла, например "file_benchmark.feather"
        result: Словарь, в который будем записывать полученные значения

    Returns: None

    """
    with Timer() as timer:
        save_ram_delta_mb = save_data_frame(data_frame, format, filename)
    result['Размер файла, Мбайт'] = round(os.stat(rf'test_formats\{filename}').st_size / 1024 ** 2, 2)
    result['RAM при сохранении файла, Мбайт'] = save_ram_delta_mb[1]
    result['Время сохранения, с'] = float(timer)


def process_load_data(format: str, filename: str, result: dict) -> None:
    """
    Функция, для того чтобы запускать load_data_frame отдельным процессом - для корректного получения значений
    Args:
        format: Формат файла
        filename: Имя файла, например "file_benchmark.feather"
        result: Словарь, в который будем записывать полученные значения

    Returns:None

    """
    with Timer() as timer:
        load_ram_delta_mb = load_data_frame(format, filename)
    result['RAM при загрузки файла, Мбайт'] = load_ram_delta_mb[1]
    result['Время загрузки, с'] = float(timer)


def benchmark(formats_list: list, count_rows: int, count_columns_number: int, count_columns_string: int, rounds: int,
              as_category: bool):
    """
    Функция запуска бенчмарка.
    Запускаем цикл раундов проверок, генерируем Data Frame и прогоняем каждый формат на запись и загрузку
    отдельными процессами. Получаем словарь с данными: формат, размер файла, потребление оперативной памяти при
    сохранении файла, время сохранения, потребление оперативной памяти при загрузке файла, время загрузки.
    Args:
        formats_list: Список форматов
        count_rows: Количество строк, генерируемого Data Frame
        count_columns_number: Количество столбцов с числами, генерируемого Data Frame
        count_columns_string: Количество столбцов со строками, генерируемого Data Frame
        rounds: Количество раундов (сколько раз будем получать данные по формату, для усреднения результата)
        as_category: True - если необходимо привести к типу данных "category"

    Returns: Data Frame, с усредненными результатами

    """
    benchmark = []
    manager = Manager()
    for i in range(rounds):
        print(f'Начинаем тест, раунд № {i + 1}')
        print('\tГенерируем Data Frame')
        data_frame = generate_data_frame(count_rows, count_columns_number, count_columns_string, as_category)

        for format in formats_list:
            # Запускаем с garbage collector
            with GC():
                print(f'\tТестируем формат: {format}')
                result = manager.dict()
                result['Формат'] = format

                # У excel и HDF расширение формата отличается от имени - приводим к выполнению функции
                if format == 'excel':
                    filename = 'file_benchmark.xlsx'
                elif format == 'hdf':
                    filename = 'file_benchmark.h5'
                else:
                    filename = f'file_benchmark.{format}'
                # Процесс сохранения Data Frame в тестируемый формат
                p_1 = Process(target=process_save_data, args=(format, data_frame, filename, result))
                p_1.start()
                p_1.join()
                # Процесс загрузки Data Frame из тестируемого формата
                p_2 = Process(target=process_load_data, args=(format, filename, result))
                p_2.start()
                p_2.join()

                result = dict(result)
                benchmark.append(pandas.DataFrame([result]))
                os.remove(rf'test_formats\{filename}')

    benchmark_result = pandas.concat(benchmark, ignore_index=True)
    benchmark_result = benchmark_result.groupby('Формат').mean()
    return benchmark_result
