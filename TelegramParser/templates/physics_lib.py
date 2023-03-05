"""
Шаблоны для парсинга телеграм каналов
"""
from TelegramParser.parser import check_repost, check_document, check_photo, \
    get_text, get_links_from_message, check_type_file, check_file_size, \
    type_file, get_year

from DatabaseTools.connect import DB
from TelegramParser.parser import TelegramConnect
from telethon.tl.patched import Message
from datetime import datetime
from Utils.plugins import image_thumbnail, image_resize_height


def checker_physics_lib(message: Message,
                        type_file_download: list,
                        limit_file_size: int) -> bool:
    """
    Функция проверки соответствия файла типу и максимальному размеру.
    Возвращает True/False в зависимости от того удовлетворяет условиям или нет

    :param message: сообщение
    :param type_file_download: список разрешенных типов файлов, ['rar', 'pdf']
    :param limit_file_size: максимальный размер файла для скачивания
    :return: True/False
    """
    flag_check = False
    if check_type_file(message, type_file_download):
        if check_file_size(message, limit_file_size):
            flag_check = True
    return flag_check


def filtering_links_physics_lib(message: Message,
                                telegram_connect: TelegramConnect,
                                friendly_channels: list,
                                pattern: str) -> list:
    """
    Функция проверяет находятся ли в теле сообщения ссылки на разрешенные
    Вами список разрешенных каналов. Соответственно в friendly_channels
    передается список этих ресурсов(username). Переменная pattern содержит
    шаблон по которому происходит отбор(для телеграма он един)

    :param message: сообщение
    :param telegram_connect: класс с подключеным Telegram
    :param friendly_channels: список разрешенных каналов (
    например, ['channel1', 'ch3'])
    :param pattern: шаблон отбора ссылок, (например, 'https://t.me/\S+/\d+')
    :return: список отфильтрованных ссылок
    """
    links = get_links_from_message(message, pattern=pattern)
    filter_links = []
    if len(links):
        for link in links:
            message_link = telegram_connect.get_message(link['username'],
                                                        link['message_id']
                                                        )
            if message_link:
                channel_id = message_link.peer_id.channel_id
                if channel_id in friendly_channels:
                    filter_links.append(message_link)
    return filter_links


def downloader_physics_lib(message: Message, telegram_connect: TelegramConnect,
                           path_download: str) -> (str, bool):
    """
    Функция загрузки файла из сообщения. Использует метод
    TelegramConnect.download_file(). Возвращает название загруженного файла и
    статус загрузки.

    :param message: сообщение
    :param telegram_connect: класс с подключеным Telegram
    :param path_download: путь для сохранения файла,
    (например, r'../Media/Downloads/')
    :return: список отфильтрованных ссылок
    """
    file_name = None
    corresponds_params = False

    if telegram_connect.download_file(message, path_download):
        file_name = message.file.name
        corresponds_params = True

    return file_name, corresponds_params


def write_db_physics_lib(message: Message, database_connect: DB,
                         telegram_connect: TelegramConnect,
                         channel_id: int, record: dict,
                         table: str, t_me_link: str) -> bool:
    """
    Функция для первичной записи данных в основную таблицу БД. Функция
    извлекает текст, название канала год и остальные данные из сообщения и
    записывает их в заданную таблицу БД.

    :param message: сообщение
    :param database_connect: класс для работы с БД
    :param telegram_connect: класс с подключеным Telegram
    :param channel_id: id канала
    :param record: словарь данных для записи
    :param table: название основной таблицы
    :param t_me_link: шаблон адреса, 'https://t.me'
    :return: True/False
    """

    try:
        # получаем имя канала и юзернейм(уникальное имя)
        channel = telegram_connect.get_channel_name(channel_id)
        username = telegram_connect.get_channel_username(channel_id)

        record['name'], record['description'] = get_text(message)
        record["channel"] = channel
        record["channel_id"] = channel_id
        record['message_id'] = message.id
        record['document_id'] = message.document.id
        record['file_size'] = message.document.size
        # record['type_file'] = message.file.name.split('.')[-1]
        record["name_link"] = f'{t_me_link}/{username}/{message.id}',
        record['date'] = datetime.now()
        record['year'] = get_year(record['description'])

        print(record)
        database_connect.insert_record(table=table, dictionary=record)
        print(f'Сообщение {message.id} добавлено в базу данных {table}!')

        return True
    except Exception as ex:
        print(ex)
        print(f'Сообщение {message.id} сохранить в {table} не удалось!')
        return False


def write_service_info_db_physics_lib(message: Message, database_connect: DB,
                                      channel_id: int, info: dict, table: str):
    """
    Функция для записи статуса по сообщению сервис таблицу БД. Функция
    извлекает текст, название канала год и остальные данные из сообщения и
    записывает их в заданную таблицу БД.
    Словарь со статусами операций над сообщением вида
    {"corresponds_params": bool, "complete": bool}

    :param message: сообщение
    :param database_connect: класс для работы с БД
    :param channel_id: id канала
    :param info: словарь содержащий
    :param table: название сервисной таблицы
    """
    try:
        info["channel_id"] = channel_id
        info["message_id"] = message.id
        info["date"] = datetime.now()

        database_connect.insert_record(table=table, dictionary=info)
        print(
            f'Запись {channel_id} {message.id} '
            f'добавлено в базу данных {table}!')

    except Exception as ex:
        print(ex)
        print(info)
        print(
            f'Запись {channel_id} {message.id} в базу '
            f'данных {table} незавершена!')


def physics_lib(database_connect: DB, telegram_connect: TelegramConnect,
                table: str, service_table: str,
                path_photo: str, path_download: str,
                limit_file_size: int, type_file_download: list,
                t_me_link: str, pattern: str,
                channel_id=1360755573):
    """
    Функция шаблон для парсинга сообщений телеграмм канала
    с channel_id=1360755573. Содержит пример логики отбора и фильтраций
    сообщений, работы с БД и Telegram.

    :param database_connect: класс для работы с БД
    :param telegram_connect: класс с подключеным Telegram
    :param service_table: название сервисной таблицы
    :param table: название основной таблицы
    :param path_photo: путь для скачивания фото
    :param path_download: путь для скачивания файлов
    :param limit_file_size: максимальный размер файла
    :param type_file_download: типы файлов
    :param t_me_link: стандартный адрес телеграмма
    :param pattern: шаблон фильтрации ссылок
    :param channel_id: id канала парсинга
    :return:
    """

    # получаем последнее спарсенное сообщение
    last_post = database_connect.get_last_post(service_table, channel_id)

    # получаем ссылки соответствующие паттерну(фильтруем ненужные ссылки)
    friendly_chs = database_connect.get_friendly_channels(
        'friendly_channels', column='channel_id')
    friendly_chs = [elems[0] for elems in friendly_chs]

    # получаем все сообщения после последнего спарсенного сообщения
    messages = telegram_connect.client.iter_messages(channel_id,
                                                     min_id=last_post,
                                                     reverse=True)
    for message in messages:
        # создаем словари с полями соответсвующими полям БД.
        record = {
            "name": None,
            "author": None,
            "description": None,
            "tags": None,
            "channel": None,
            "channel_id": None,
            "message_id": None,
            "document_id": None,
            "date": None,
            "name_link": f'{t_me_link}/',
            "file_name": None,
            "file_size": None,
            "photo": False,
            "photo_link": None,
            "photo_resize": None,
            "photo_thumbnail": None,
            "public_tg": False,
            "date_tg": None,
            "public_site": False,
            "date_site": None,
            "category": None,
            "yadisk": None,
            "type_file": None
        }

        service_info = {
            "channel_id": None,
            "message_id": None,
            "corresponds_params": False,
            "complete": False,
            "date": None
        }
        # "corresponds_params": True - если удовлетворяет условиям
        # "complete": True - если удовлетворяет условиям и был скачан,
        # в останых случаях False

        # проверяем сообщение на репост и на наличие записи в БД.
        if check_repost(message) and not database_connect.check_record(
                channel_id, message.id, service_table):

            # проверяем наличие документа в сообщении
            if check_document(message):
                # проверяем на размер файла и тип
                if checker_physics_lib(message, type_file_download,
                                       limit_file_size):
                    # загружаем файл и записываем имя файла в словарь,
                    # записываем статус операции в сответствующий словарь

                    record['file_name'], service_info["corresponds_params"] \
                        = downloader_physics_lib(message, telegram_connect,
                                                 path_download)
                    record['type_file'] = type_file(record['file_name'])

                # заполняем необходимые ключи в словаре и записываем в БД,
                # записываем статус операции в сответствующий словарь

                service_info["complete"] = write_db_physics_lib(
                    message,
                    database_connect,
                    telegram_connect,
                    channel_id,
                    record,
                    table,
                    t_me_link
                )

            elif check_photo(message):

                # получаем сообщения из ссылок соответствующих паттерну
                # отфильтровываем сообщения по доверенным каналам
                f_messages = filtering_links_physics_lib(message,
                                                         telegram_connect,
                                                         friendly_chs,
                                                         pattern=pattern)
                # если после фильтрации получили 0 ссылок, скорее всего
                # исходное сообщение неподходит, производим запись только
                # в служебную БД(service_info)
                if len(f_messages):
                    # если сообщения существуют, то будут иметь одно фото
                    # загружаем фото, производим запись имени файла в словарь
                    record['photo'], record['photo_link'] \
                        = telegram_connect.download_photo(message, path_photo)

                    # создаем thumbnail
                    path_photoname = path_photo + record['photo_link']
                    record["photo_thumbnail"] = image_thumbnail(
                        path_photoname)
                    record["photo_resize"] = image_resize_height(
                        path_photoname)

                    for f_message in f_messages:
                        f_channel_id = f_message.peer_id.channel_id
                        # проверяем запись в БД
                        if not database_connect.check_record(f_channel_id,
                                                             f_message.id,
                                                             service_table):

                            # проверяем наличие документа в сообщении
                            if check_document(f_message):

                                # проверяем на размер файла и тип
                                if checker_physics_lib(f_message,
                                                       type_file_download,
                                                       limit_file_size):
                                    # загружаем файл
                                    record['file_name'], \
                                        service_info["corresponds_params"] \
                                        = downloader_physics_lib(
                                        f_message,
                                        telegram_connect,
                                        path_download)

                                    record['type_file'] = type_file(
                                        record['file_name'])

                                # заполняем функцией необходимые ключи в
                                # словаре и
                                # записываем в БД.
                                service_info["complete"] \
                                    = write_db_physics_lib(f_message,
                                                           database_connect,
                                                           telegram_connect,
                                                           f_channel_id,
                                                           record, table,
                                                           t_me_link)
                            # записать в базу со парсенными сообщениями
                            # сообщением
                            write_service_info_db_physics_lib(f_message,
                                                              database_connect,
                                                              f_channel_id,
                                                              service_info,
                                                              service_table)
        write_service_info_db_physics_lib(message, database_connect,
                                          channel_id, service_info,
                                          service_table)
