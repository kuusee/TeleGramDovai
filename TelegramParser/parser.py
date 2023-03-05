from typing import List

from telethon.sync import TelegramClient
from telethon.tl.patched import Message
from telethon.tl.types import MessageEntityTextUrl, MessageMediaPhoto
from telethon.tl.types import MessageMediaDocument
from datetime import datetime
from tqdm import tqdm
import re


class TelegramConnect:
    """
    Класс для подключения к Telegram и набором методов для работы
    с сообщениями.

    """

    def __init__(self, api_id, api_hash, session='session_name'):

        self.client = TelegramClient(session, api_id, api_hash)
        self.pbar = None
        self.prev_current = 0
        self.client.start()

    def get_message(self, channel_attr, message_id: int) -> (Message, None):
        """
        Получить сообщение с заданным id=message_id из
        канал с id=channel_id

        :param channel_attr:
        :param message_id: номер сообщения в канале (например, 213)
        :return: Message | None
        """
        try:
            message = self.client.iter_messages(channel_attr, ids=message_id)
            return message.__next__()
        except Exception as exc:
            print(exc,
                  f'\nСгенерировано для сообщения {message_id} из канала '
                  f'{channel_attr}\n',
                  'Возвращено None.')
            return None

    def get_messages(self, channel_id: int, min_id: int) -> List[Message]:
        """
        Получить пакет сообщений из заданного канала с id=channel_id от
        сообщения с заданным номером min_id и до заданного номера max_id.
        max_id=0 - до последнего.

        :param channel_id: id канала (например, 12345)
        :param min_id: минимальный номер сообщения (например, 12)
        :return: список сообщений
        """

        peer_channel = self.client.get_entity(channel_id)
        messages = self.client.iter_messages(peer_channel, min_id=min_id,
                                             reverse=True)
        return messages

    def get_channel_name(self, channel_id) -> str:
        """
        Получить имя канала.

        :param channel_id: id канала (например, 12345)
        :return: название канала
        """
        return self.client.get_entity(channel_id).title

    def get_channel_username(self, channel_id) -> str:
        """
        Получить юзернейм канала.
        """
        return self.client.get_entity(channel_id).username

    def download_file(self, msg: Message, path: str) -> bool:
        """
        Загрузка файла из сообщения по указанному пути.

        :param msg: экземпляр класса Message
        :param path: путь для загрузки файла (например, '/Media/Downloads/')
        :return: True/False
        """
        try:
            self.prev_current = 0
            self.pbar = tqdm(total=msg.document.size, desc=msg.file.name,
                             unit='B', unit_scale=True, colour='green')
            self.client.download_media(msg, file=path + msg.file.name,
                                       progress_callback=self.__callback)
            self.pbar.close()
            del self.pbar
            return True
        except Exception as exc:
            print(exc, f'Проблемы с загрузкой файла {msg.file.name} из '
                  f'channel_id:{msg.peer_id.channel_id} msg_id: {msg.id}')
            # дописать проверку на недогруженный файл
            return False

    def download_photo(self, msg: Message, path: str) -> (bool, str):
        """
        Сохраняет фото из сообщения по заданному пути.
        Имя сохраняемого фото в виде:
        '{msg.peer_id.channel_id}_{msg.id}_{datetime.now().microsecond}.jpg'

        Создает ключ-значение в переданном словаре 'PHOTO', 'PHOTO_LINK'
        соответствующие полям БД.

        :param msg: экземпляр класса Message
        :param path: путь для загрузки изображения (например, '/Media/Photo/')
        :return: Tuple[True, название_файла]
        """
        name_photo = '{}_{}_{}.jpg'.format(
            msg.peer_id.channel_id, msg.id, datetime.now().microsecond)
        self.client.download_media(msg, file=path + name_photo)
        return True, name_photo

    def __callback(self, current, total):
        self.pbar.update(current - self.prev_current)
        self.prev_current = current


def get_links_from_message(msg: Message, pattern: str) -> list:
    """
    Функция получает из тела сообщения ссылки по заданному паттерну.

    :param msg: экземпляр класса Message
    :param pattern: шаблон поиска ссылок
    :return: list({'username': username, "message_id": message_id}, ...)
    """
    message_entity = []
    if msg.entities:
        for entity in msg.entities:
            if isinstance(entity, MessageEntityTextUrl):
                if re.fullmatch(pattern, entity.url):
                    username = entity.url.split('/')[-2]
                    message_id = int(entity.url.split('/')[-1])
                    message_entity.append({'username': username,
                                           "message_id": message_id}
                                          )
    return message_entity


def delete_emoji(text: str) -> str:
    """
    Удаляет emoji из текста.
    """
    emoji_pattern = re.compile("["
                               u"\U0001F600-\U0001F64F"  # emoticons
                               u"\U0001F300-\U0001F5FF"  # symbols,pictographs
                               u"\U0001F680-\U0001F6FF"  # transport,map symb
                               u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                               "]+", flags=re.UNICODE)
    return emoji_pattern.sub(r'', text)


def get_text(message: Message) -> (str, str):
    """
    Получаем заголовок и описание из сообщения.

    :param message: экземпляр класса Message
    :return: Tuple[title, description]
    """
    title = delete_emoji(str(message.message.split('\n\n')[0][:255]))
    description = delete_emoji(str(message.message.strip('\n')))
    return title, description


def get_year(text: str, min_year=1800, max_year=2023) -> int:
    """
    Извлекает год из текста по заданному шаблону. Если дат несколько
    вернет более ранний год. Диапозон дат 1800-2023.

    :param text: текст для извлечения (например,
    "Извлекает [1990], 1802 из ...")
    :param min_year: минимальный год(например, 1800)
    :param max_year: минимальный год(например, 2022)
    :return: минимальный год или None
    """
    year = [int(x) for x in re.findall('\d\d\d\d', text)]
    year = [x for x in year if min_year <= x <= max_year]
    return min(year) if year else None


def check_repost(msg: Message) -> bool:
    """
    Проверяет является ли сообщение репостом.

    :param msg: экземпляр класса Message
    :return: True/False
    """
    return True if not msg.fwd_from and len(str(msg.message)) else False


def check_photo(msg: Message) -> bool:
    """
    Проверяет на наличие фотографии в сообщении.

    :param msg: экземпляр класса Message
    :return: True/False
    """
    return True if isinstance(msg.media, MessageMediaPhoto) else False


def check_document(msg: Message) -> bool:
    """
    Проверяет на наличие документа в сообщении.

    :param msg: экземпляр класса Message
    :return: True/False
    """
    return True if isinstance(msg.media, MessageMediaDocument) else False


def check_type_file(msg: Message, type_files: list) -> bool:
    """
    Проверяет соответствие требуемого типа файла из сообщения.

    :param msg: экземпляр класса Message
    :param type_files: список разрешенных расширений файлов
    (например, ['rar', 'zip', ...])
    :return: True/False
    """
    file_format = msg.file.name.split('.')[-1]
    return True if file_format in type_files else False


def check_file_size(msg: Message, limit_file_size: int) -> bool:
    """
    Проверяет размер файла в сообщении с максимально допустимым.

    :param limit_file_size:
    :param msg: экземпляр класса Message
    :param limit_file_size: максимальный размер файла в байтах
    (например, 15728640)
    :return: True/False
    """
    return True if msg.document.size <= limit_file_size else False


def type_file(file_name: str) -> str:
    """
    Вытаскивает тип файла.

    :param file_name: название файла
    :return: тип файла (например, pdf)
    """
    return file_name.rsplit('.')[-1]
