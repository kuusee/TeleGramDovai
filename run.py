from TelegramParser.parser import TelegramConnect
from TelegramParser.templates import physics_lib

from DatabaseTools.connect import DB
from DatabaseTools import schemas

from YandexDiskKeeper.keeper import YaDiskStorage

from Utils.plugins import create_path, Sftp

from decouple import config
from tqdm import tqdm


T_ME_LINK = 'https://t.me'

# Пути для сохранения файлов
PATH_DOWNLOAD = r'../Media/Downloads/'
PATH_PHOTO = r'../Media/Photo/'

# Путь для сохранения изображений на удаленном сервере
PATH_REMOTE_PHOTO = './Media/Files/Photo'

# Путь для сохранения файлов на ЯндексДиске
YADISK_DOWNLOAD = r'/Media/Downloads/'

# Максимальный размер закачиваемого файла и его типы
LIMIT_FILE_SIZE = 15728640  # bytes (15Mb) # 78643200  # bytes (75Mb)
TYPE_FILE_DOWNLOAD = ['rar', 'pdf', 'djvu', 'zip', '7z']

# Шаблон фильтра ссылок(оставляет только телеграм ссылки)
PATTERN = 'https://t.me/\S+/\d+'

# Шаблон поиска изображений
NAME_FILE_PHOTO_PATTERN = '\d+_\d+_\d+_resize.jpg|\d+_\d+_\d+_thumbnail.jpg'

# Названия таблиц в БД
MAIN_TABLE = 'book_books'
SERVICE_TABLE = 'service_info'
FRIENDLY_CHANNELS_TABLE = 'friendly_channels'


def main():
    # создаем соединение с Телеграм
    tg = TelegramConnect(api_id=config('TELEGRAM_API_ID'),
                         api_hash=config('TELEGRAM_API_HASH'),
                         session='session_name'
                         )

    # создаем подключение к базе данных
    db = DB(database=config('DATABASE_NAME'),
            user=config('DATABASE_USERNAME'),
            password=config('DATABASE_PASSWORD'),
            host=config('DATABASE_HOST'),
            sslmode='verify-ca',
            sslrootcert=config('PATH_DATABASE_CERT')
            )

    # db = DB(database='pyapp', user='kuusee', password='357612462')

    # создаем объект класса для работы по sftp
    sftp_upload = Sftp(config('SFTP_USER'), config('SFTP_PASSWORD'),
                       config('SFTP_HOST'), int(config('SFTP_PORT')))

    # создаем подключение к ЯндексДиску
    storage = YaDiskStorage(config('YADISK_TOKEN'))

    # создаем/проверяем пути
    create_path(PATH_DOWNLOAD)
    create_path(PATH_PHOTO)
    storage.create_dirs(YADISK_DOWNLOAD)

    # создаем требуемые таблицы в БД
    db.create_table(MAIN_TABLE, schemas.MAIN_TABLE)
    db.create_table(SERVICE_TABLE, schemas.SERVICE_INFO)
    db.create_table(FRIENDLY_CHANNELS_TABLE, schemas.FRIENDLY_CHANNELS)

    # Пар
    physics_lib.physics_lib(database_connect=db, telegram_connect=tg,
                            table=MAIN_TABLE, service_table=SERVICE_TABLE,
                            path_photo=PATH_PHOTO, path_download=PATH_DOWNLOAD,
                            limit_file_size=LIMIT_FILE_SIZE,
                            type_file_download=TYPE_FILE_DOWNLOAD,
                            t_me_link=T_ME_LINK, pattern=PATTERN)

    # загружаем изображения по sftp на удаленный сервер
    sftp_upload.upload_files(PATH_PHOTO,
                             PATH_REMOTE_PHOTO,
                             NAME_FILE_PHOTO_PATTERN)

    # создаем список файлов для загрузки на яндекс диск
    pbar = tqdm(db.select_null_yadisk(MAIN_TABLE))  # красивый бар загрузки
    for pk, file, _ in pbar:
        os_path = PATH_DOWNLOAD + file
        pbar.set_description(f"Processing '{os_path}'")
        href = storage.upload_file(os_path, YADISK_DOWNLOAD, file)
        db.set_values('book_books', {'id': pk, 'yadisk': href})


if __name__ == '__main__':
    main()
