# Лучший формат для хранения Data Frame pandas
Данный бенчмарк позволяет проанализировать и выбрать подходящий, под ваши задачи формат хранения данных, с которыми Pandas работает "из коробки". 
В качестве сравнения используются следующие метрики: размер сериализованного файла, время затраченное на загрузку Data Frame из файла, время затраченное 
на сохранение Data Frame в файл, потребление оперативной памяти при сохранении и загрузки Data Frame. 
Также сравнение производится при типе данных "object" и типе данных "category"  

### Реализация
Генерация численных данных проводилась с помощью numpy.random.normal, за строчные данные использовались UUID. 
Сбор метрики RAM, проводится с помощью функции декоратора, для чистоты данных функции загрузки и сохранения поделены на разные процессы.
Сбор метрики времени, осуществляется с помощью контекстного менеджера. Им же, после каждого цикла запускается garbage collector.
Результаты сохраняются в excel, для возможной дальнейшей обработки.

### Основной процесс
В качестве примера, в main функции, представлен бенчмарк, который генерирует Data Frame с 1 миллионом строк, 15 столбцами цифр и 15 столбцами строковых значений.
Тестируемые форматы: csv, json, pickle, feather, parquet, hdf.
Метрики собирались в течении 5 раундов, после чего группировались в результирующий Data Frame по среднему значению. 
```python
  format_list = ['csv', 'json', 'pickle', 'feather', 'parquet', 'hdf']
  df_result = benchmark(format_list, 1_000_000, 15, 15, 5, False)
```
### Результаты бенчмарка, в котором тип данных "object" не преобразовывался к типу "category" 
![Image alt](https://github.com/V-Moskalenko/formats_benchmark_to_pandas_data/blob/master/test_formats/Время%2C%20тип%20данных%20строки.png)
![Image alt](https://github.com/V-Moskalenko/formats_benchmark_to_pandas_data/blob/master/test_formats/Размер%2C%20тип%20данных%20строки.png)
![Image alt](https://github.com/V-Moskalenko/formats_benchmark_to_pandas_data/blob/master/test_formats/RAM%2C%20тип%20данных%20строки.png)

### Результаты бенчмарка, в котором тип данных "object" преобразовывался к типу "category"
```python
  df_result_2 = benchmark(format_list, 1_000_000, 15, 15, 5, True)
```
![Image alt](https://github.com/V-Moskalenko/formats_benchmark_to_pandas_data/blob/master/test_formats/Время%2C%20тип%20данных%20категории.png)
![Image alt](https://github.com/V-Moskalenko/formats_benchmark_to_pandas_data/blob/master/test_formats/Размер%2C%20тип%20данных%20категории.png)
![Image alt](https://github.com/V-Moskalenko/formats_benchmark_to_pandas_data/blob/master/test_formats/RAM%2C%20тип%20данных%20категории.png)

### requirements (необходимые библиотеки):
* contourpy==1.0.5
* cycler==0.11.0
* et-xmlfile==1.1.0
* fonttools==4.37.4
* kiwisolver==1.4.4
* matplotlib==3.6.1
* numexpr==2.8.3
* numpy==1.23.3
* openpyxl==3.2.0b1
* packaging==21.3
* pandas==1.5.0
* Pillow==9.2.0
* psutil==5.9.2
* pyarrow==9.0.0
* pyparsing==3.0.9
* python-dateutil==2.8.2
* pytz==2022.4
* six==1.16.0
* tables==3.7.0
* uuid==1.30
