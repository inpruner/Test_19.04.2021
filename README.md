В db.db представлена БД производства.
unit:
    id:     ид установки
    name:   имя
    type:   0 - АВТ, 1 - Вторичного производства

stream:
    id:     ид потока
    name:   имя

unit_material (связь потоков и установок):
    unit_id:    ид установки
    stream_id:  ид потока
    feed_flag:  0 - выход с установки, 1 - сырье установки

load_max (максимальная загрузка установки):
    unit_id:    ид установки
    value:      максимальное значение

Задачи:
1.  Создать класс установки. Два наследника класса (АВТ/Вторичная).
    Свойства: имя, максимальное значение загрузки, словарь входных потоков с ссылкой на объекты потоков,
    словарь выходных потоков с ссылкой на объекты потоков.
    Инкапсулровать 'максимальное значение загрузки'.
2.  Создать класс потоков.
    Свойства: имя, список на каких установках вырабатывается, список на какие установки поступает.
3.  Создать запрос к БД, возвращающий входные потоки установок в форме: имя_установки, имя_потока_на_входе.
4.  Вывести в .csv потоки, которые не поступают и не выходят ни с одной установки в формате: id, имя_потока.
5.  Вывести в .json потоки, которые поступают на несколько установок в формате словаря (ключ - название потока,
    значение - список установок).
6.  Создать excel, в котором каждый лист - название установки.
    На листе в первой колонке указать входные потоки, во второй выходные (библиотека openpyxl).
