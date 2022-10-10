import matplotlib
from utils import benchmark


def main():
    # Указываем список тестируемых форматов
    format_list = ['csv', 'json', 'pickle', 'feather', 'parquet', 'hdf']

    # Запускаем бенчмарк, с 5 раундами и получаем Data Frame с результатами
    # Тестовый DF - 1 000 000 строк, 15 столбцов с цифрами, 15 столбцов со строками
    df_result = benchmark(format_list, 1_000_000, 15, 15, 5, False)

    # Сохраним результаты в excel и построим график с помощью matplotlib
    df_result.to_excel(r'test_formats\result.xlsx')

    # График времени сохранения/загрузки
    df_result.plot.bar(y=['Время сохранения, с', 'Время загрузки, с'], legend=True,
                       title='Время сохранения/загрузки, тип данных - строки', figsize=(10, 11))
    matplotlib.pyplot.savefig(r'test_formats\Время, тип данных строки')

    # График размера файла
    df_result.plot.bar(y='Размер файла, Мбайт', legend=True, title='Размер файла, тип данных - строки',
                       figsize=(10, 11))
    matplotlib.pyplot.savefig(r'test_formats\Размер, тип данных строки')

    # График изменения оперативной памяти при сохранении/загрузке
    df_result.plot.bar(y=['RAM при сохранении файла, Мбайт', 'RAM при загрузки файла, Мбайт'], legend=True,
                       title='RAM сохранения/загрузки, тип данных - строки', figsize=(10, 11))
    matplotlib.pyplot.savefig(r'test_formats\RAM, тип данных строки')

    # Запускаем бенчмарк, с преобразованием к типу данных "category"
    df_result_2 = benchmark(format_list, 1_000_000, 15, 15, 5, True)
    df_result_2.to_excel(r'test_formats\result_2.xlsx')

    # График времени сохранения/загрузки
    df_result_2.plot.bar(y=['Время сохранения, с', 'Время загрузки, с'], legend=True,
                         title='Время сохранения/загрузки, тип данных - категории', figsize=(10, 11))
    matplotlib.pyplot.savefig(r'test_formats\Время, тип данных категории')

    # График размера файла
    df_result_2.plot.bar(y='Размер файла, Мбайт', legend=True, title='Размер файла, тип данных - категории',
                         figsize=(10, 11))
    matplotlib.pyplot.savefig(r'test_formats\Размер, тип данных категории')

    # График изменения оперативной памяти при сохранении/загрузке
    df_result_2.plot.bar(y=['RAM при сохранении файла, Мбайт', 'RAM при загрузки файла, Мбайт'], legend=True,
                         title='RAM сохранения/загрузки, тип данных - категории', figsize=(10, 11))
    matplotlib.pyplot.savefig(r'test_formats\RAM, тип данных категории')


if __name__ == '__main__':
    main()
