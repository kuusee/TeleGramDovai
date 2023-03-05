from PIL import Image
import hashlib
import os
import re
import pysftp
from tqdm import tqdm


class Sftp:
    """
    Класс для работы с удаленным сервером по протоколу sftp.

    """
    def __init__(self, username: str, password: str, host: str, port: int):
        self.user = username
        self.pswd = password
        self.host = host
        self.port = port

    @staticmethod
    def get_local_list(path_files: str, find_pattern='') -> set:
        """
        Функция создает список файлов лежащих в папке по заданному шаблону.
        :param path_files: путь к локальной директории с файлами для
        загрузки (например, '../Media/Photo/')
        :param find_pattern: паттерн для отбора файлов
        (например, '\d+_\d+_\d+_resize.jpg') по умолчанию ''
        :return: Множество файлов
        """
        transfer_list = set()

        for file in os.listdir(path_files):
            if re.findall(find_pattern, file):
                transfer_list.add(file)
        return transfer_list

    def upload_files(self, path_local_dir: str,
                     path_remote_dir: str,
                     find_pattern='') -> None:
        """
        Загрузка файлов на удаленный сервер по протоколу SFTP. Создает список
        файлов для заданной папки, фильтрует их по заданному шаблону,
        сопоставляет их по названию с уже имеющимися на удаленном сервере и
        в случае отсутствия загружает эти файлы.

        :param path_local_dir: путь к локальной директории с файлами для
        загрузки (например, '../Media/Photo/')
        :param path_remote_dir: путь к директории на удаленном сервере
        (например, './Photo_media/Photo')
        :param find_pattern: паттерн для отбора файлов
        (например, '\d+_\d+_\d+_resize.jpg') по умолчанию ''
        :return: None
        """

        # подключаемся к удаленному серверу
        with pysftp.Connection(username=self.user,
                               password=self.pswd,
                               host=self.host,
                               port=self.port) as sftp:

            # меняем директорию на заданную
            with sftp.cd(path_remote_dir):

                # получаем список файлов на локальной машине
                local_list = self.get_local_list(path_local_dir,
                                                 find_pattern)
                # получаем список файлов на удаленной машине
                remote_list = set(sftp.listdir())
                # выбрасываем из списка файлов на локальной машине файлы
                # которые уже имеются на удаленной
                transfer_list = local_list - remote_list

                # бар загрузки
                pbar = tqdm(transfer_list, unit='files', colour='green')

                # в цикле по одному файлу загружаем на удаленный сервер
                for file in pbar:
                    path_local_file = path_local_dir + file
                    pbar.set_description(f"Processing '{path_local_file}'")
                    sftp.put(path_local_file, file)


def checksum_md5(path_file: str) -> str:
    """
    Вычисляет хеш-сумму файла.
    :param path_file: путь к файлу (например, '../Media/Downloads/file1.rar')
    :return: хеш-сумма
    """
    hash_md5 = hashlib.md5()
    with open(path_file, "rb") as file:

        for chunk in iter(lambda: file.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def create_path(path: str) -> None:
    """
    Создает путь на локальном диске.
    :param path: путь (например, '../Media/Downloads/')
    :return:
    """
    if os.path.exists(path):
        print(f'OS:: путь существует: {path}')
    else:
        os.makedirs(path)
        print(f'OS:: путь создан: {path}')


def image_thumbnail(path: str, size=(300, 300), ) -> str:
    """
    Создает thumbnail изображения с заданым размером. Сохраняет по тому же пути
    :param path: путь (например, '../Media/Downloads/1234.jpg')
    :param size: кортеж, размер thumbnail (по умолчанию size=(300, 300))
    :return: название файла thumbnail (например, '1234_thumbnail.jpg')
    """
    path_split = path.rsplit('.', maxsplit=1)
    path_output = path_split[0] + '_thumbnail' + '.' + path_split[1]
    img = Image.open(path)
    img.thumbnail(size=size)
    img.save(path_output)
    return path_output.rsplit('/', maxsplit=1)[-1]


def image_resize_height(path: str, height=400):
    """
    Уменьшает изображение с сохранением пропорций для заданной высоты.
    Сохраняет по тому же пути.
    :param path: путь (например, '../Media/Downloads/1234.jpg')
    :param height: высота выходного изображения (по умолчанию height=400)
    :return: название файла (например, '1234_resize.jpg')
    """
    path_split = path.rsplit('.', maxsplit=1)
    path_output = path_split[0] + '_resize' + '.' + path_split[1]

    img = Image.open(path)
    new_width = int(height * img.width / img.height)
    new_img = img.resize((new_width, height), Image.ANTIALIAS)
    new_img.save(path_output)
    return path_output.rsplit('/', maxsplit=1)[-1]
