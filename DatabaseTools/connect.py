import psycopg2
import psycopg2.extras
from psycopg2.extras import RealDictCursor
from psycopg2 import sql


class DB:
    """
    Класс для работы с БД в проекте с необходимым функционалом.
    По умолчанию использует локальный сервер '127.0.0.1', порт 5432,
    sslmode='require' и sslrootcert=''.
    """
    def __init__(self,
                 database: str,
                 user: str,
                 password: str,
                 host='127.0.0.1',
                 port=5432,
                 sslmode='require',
                 sslrootcert=''
                 ) -> None:

        # https://help.compose.com/docs/postgresql-and-python
        # https://stackoverflow.com/questions/28228241/how-to-connect-to-a
        # -remote-postgresql-database-through-ssl-with-python

        self.con = psycopg2.connect(database=database,
                                    user=user,
                                    password=password,
                                    host=host,
                                    port=port,
                                    sslmode=sslmode,
                                    sslrootcert=sslrootcert
                                    )

    def create_table(self, table: str, schema: list) -> None:
        """
        Создает таблицу в БД.

        :param table: название таблицы (например, "main_mains")
        :param schema: список подстрок SQL-запроса создания столбцов
        (например, ["ID serial primary key", "CHANNEL_ID bigint NOT NULL",
        "FILE_NAME VARCHAR(255)", ...])
        :return: None
        """

        query = sql.SQL("create table if not exists {} ({})").format(
            sql.Identifier(table),
            sql.SQL(','.join(schema))
        )
        with self.con:
            with self.con.cursor() as cur:
                cur.execute(query)
        print(f'таблица: {table} создана/существует')

    def select_all(self, table: str, output="dict") -> list:
        """
        Выбрать все записи в заданной таблице БД.

        :param table: название таблицы (например, "main_mains")
        :param output: тип элементов возвращаемого списка "
        :return: output="dict" -> list(dict(), ...)
        :return: output="tuple" -> list(tuple(), ...)
        """
        query = sql.SQL("SELECT * FROM {}").format(sql.Identifier(table))

        with self.con:
            if output == "dict":
                with self.con.cursor() as cur:
                    cur.execute(query)
                    list_all = cur.fetchall()
            elif output == "tuple":
                with self.con.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(query)
                    list_all = cur.fetchall()
        return list_all

    def insert_record(self, table: str, dictionary: dict) -> bool:
        """
        Запись данных в заданную таблицу БД с переменным числом параметров.

        Полезные ссылки:
        https://www.psycopg.org/docs/sql.html#module-usage

        Пример.
        table="main_mains"
        dictionary = {"foo": True, "bar": 777, "baz": 'Universium'}
        query -> "INSERT INTO book_books ("foo", "bar", "baz") VALUES
        (True, 777, \'Universium\')"
        return True

        :param table: название таблицы (напимер, "main_mains")
        :param dictionary: словарь, где {key=имя столбца: value=значение}
        (например, {"col1": True, "col2": 777, "title": 'Universium'})
        :return: True
        """
        columns = dictionary.keys()
        query = sql.SQL("insert into {} ({}) values ({})").format(
            sql.Identifier(table),
            sql.SQL(', ').join(map(sql.Identifier, columns)),
            sql.SQL(', ').join(map(sql.Placeholder, columns))
        )
        with self.con:
            with self.con.cursor() as cur:
                cur.execute(query, dictionary)
                self.con.commit()
        return True

    def get_last_post(self, table: str, channel_id: int) -> int:
        """
        Получить номер последнего сообщения для заданного канала.

        :param table: название таблицы (напимер, "main_mains")
        :param channel_id: id канала(например, 111111)
        :return: номер сообщения или 0
        """
        query = sql.SQL(
            "select message_id from {} where channel_id = %s "
            "order by message_id desc").format(sql.Identifier(table))

        with self.con:
            with self.con.cursor() as cur:
                cur.execute(query, (channel_id,))
                msg_id = cur.fetchone()
        return msg_id[0] if msg_id else 0

    def check_friendly_channel(self, table: str, channel_id: int) -> bool:
        """
        Проверяет наличие канала в проверенных.

        :param table: название таблицы (напимер, "friendly_channels")
        :param channel_id: channel_id: id канала(например, 111111)
        :return: True/False
        """
        query = sql.SQL(
            "select exists (select * from {} where channel_id = %s)").format(
            sql.Identifier(table)
        )

        with self.con:
            with self.con.cursor() as cur:
                cur.execute(query, (channel_id,))
                exists = cur.fetchone()[0]
        return exists

    def get_friendly_channels(self, table: str, column: str) -> list:
        """
        Получить список проверенных каналов.

        :param table: название таблицы (напимер, "friendly_channels")
        :param column: название столбца (напимер, "channel_id")
        :return: список каналов
        """
        query = sql.SQL("select {} from {}").format(
            sql.Identifier(column),
            sql.Identifier(table)
        )
        with self.con:
            with self.con.cursor() as cur:
                cur.execute(query)
                list_friendly = cur.fetchall()
        return list_friendly

    def check_record(
            self, channel_id: int, message_id: int, table: str) -> bool:
        """
        Проверяет наличие записи в БД по заданному каналу и
        номеру сообщения в нем.

        :param table:название таблицы (напимер, "main_mains")
        :param channel_id: id канала(например, 111111)
        :param message_id: id сообщения в канале (например, 123)
        :return: True/False
        """
        query = sql.SQL("select exists (select * from {} "
                        "where channel_id = %s and message_id = %s)").format(
            sql.Identifier(table))

        values = (channel_id, message_id)
        with self.con:
            with self.con.cursor() as cur:
                cur.execute(query, values)
                exists = cur.fetchone()[0]
        return exists

    def set_values(self, table: str, dictionary: dict) -> bool:
        """
        Изменить данные по PK в заданной таблице БД с переменным числом
        параметров. PK передается в словаре по ключу 'id'.

        Полезные ссылки:
        https://www.psycopg.org/docs/sql.html#module-usage

        Пример.
        table="main_mains"
        dictionary = {"id": 61, "foo": 888, "baz": 'Dark'}
        query -> "UPDATE "book_books" SET ("foo", "baz") = (888, \'Dark\')
        WHERE ID = 61"
        return True

        :param table: название таблицы (напимер, "main_mains")
        :param dictionary: словарь изменяемых параметров, должен содержать id
        (например, {"id": 61, "foo": 888, "baz": 'Dark'})
        :return: True/False
        """
        flag_complete = False
        if dictionary.get('id', False):
            columns = dictionary.keys()

            query = sql.SQL("UPDATE {} SET ({}) = ({}) WHERE ID = {}").format(
                sql.Identifier(table),
                sql.SQL(', ').join(map(sql.Identifier, columns)),
                sql.SQL(', ').join(map(sql.Placeholder, columns)),
                sql.Placeholder('id')
            )

            with self.con:
                with self.con.cursor() as cur:
                    cur.execute(query, dictionary)
                    self.con.commit()
            flag_complete = True
        return flag_complete

    def select_null_yadisk(self, table: str) -> list:
        """
        Получить список файлов не имеющих ссылку на ЯндексДиск.

        :param table: название таблицы (напимер, "main_mains")
        :return: list(Tuple[id, file_name, yadisk], ...)
        """

        query = sql.SQL("SELECT ID, FILE_NAME, YADISK FROM {} WHERE "
                        "FILE_NAME IS NOT NULL AND YADISK IS NULL;").format(
            sql.Identifier(table))

        with self.con:
            with self.con.cursor() as cur:
                cur.execute(query)
                list_null = cur.fetchall()
        return list_null

    def get_schema(self, table: str) -> list:
        """
        Схема таблицы.

        :param table: название таблицы (напимер, "main_mains")
        :return: список
        """
        query = """SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_name = %s;"""
        with self.con:
            with self.con.cursor() as cur:
                cur.execute(query, (table,))
                list_schema = cur.fetchall()
        return list_schema
