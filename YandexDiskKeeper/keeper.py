from yadisk.yadisk import YaDisk


class YaDiskStorage(YaDisk):
    """
    Создает подключение к ЯндексДиску и позволяет производить основные операции
    с файлами и папками.

    Для получения токена нужно иметь ЯндексДиск.
    Далее по инструкции  https://yandex.ru/dev/direct/doc/start/token.html
    """
    def __init__(self, ya_token):
        super().__init__()
        self.token = ya_token

    def create_dirs(self, path: str):
        """
        Создает папки в заданном пути.
        Если путь существует, напечатает сообщение
        "YADISK:: путь существует: {path}"
        Пример.
        create_dirs(path='/Media/Downloads/') -> создаст папку "Media" ->
        создаст в папке "Media" папку "Downloads"

        :param path: создать путь (например, '/Media/Downloads/')
        :return: None
        """

        if self.exists(path):
            print(f'YADISK:: путь существует: {path}')

        else:
            tree_dirs = [dir_ for dir_ in path.split('/') if dir_]
            path_ = ''

            for dir_ in tree_dirs:
                path_ = path_ + dir_ + '/'
                if not self.exists(path_):
                    self.mkdir(path_)
                    print(f'YADISK:: создана директория: {path_}')

            print(f'YADISK:: путь создан: {path_}')

    def upload_file(self, os_path: str, ya_path: str, file: str) -> str:
        """
        Загружает файл на диск по указанному пути и делает его публичным.

        :param os_path:путь к файлу в ОС
        (например, "/Media/Downloads/file1.pdf")
        :param ya_path:путь к папке яндекс диска, куда необходимо
        загрузить (например, "/Media/Downloads/")
        :param file:имя загружаемого файла
        (например, "file1.pdf", 'new_file1.pdf' и т.д.)
        :return:публичная ссылка на загруженный файл
        """
        upload_path = ya_path + file
        if not self.is_file(upload_path):
            self.upload(os_path, upload_path, timeout=None)

        self.publish(upload_path)
        href = self.get_meta(upload_path)['public_url']
        return href
