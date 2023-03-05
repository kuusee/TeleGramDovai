# Шаблон для создания главной таблицы
MAIN_TABLE = [
                "ID serial primary key",
                "TITLE VARCHAR(255)",
                "AUTHOR VARCHAR(255)",
                "YEAR INT",
                "DESCRIPTION TEXT NOT NULL",
                "TAGS VARCHAR(255)",
                "CHANNEL VARCHAR(255)",
                "CHANNEL_ID bigint NOT NULL",
                "MESSAGE_ID bigint NOT NULL",
                "DOCUMENT_ID bigint NOT NULL",
                "DATE timestamptz NOT NULL",
                "NAME_LINK VARCHAR(255) NOT NULL",
                "FILE_NAME VARCHAR(255) NOT NULL",
                "TYPE_FILE VARCHAR(6)",
                "FILE_SIZE bigint NOT NULL",
                "PHOTO BOOL NOT NULL",
                "PHOTO_LINK VARCHAR(255)",
                "PHOTO_RESIZE VARCHAR(255)",
                "PHOTO_THUMBNAIL VARCHAR(255)",
                "PUBLIC_TG BOOL",
                "DATE_TG timestamptz",
                "PUBLIC_SITE BOOL",
                "CATEGORY VARCHAR(255)",
                "YADISK text",
            ]

# Шаблон для создания таблицы лога
SERVICE_INFO = [
                "channel_id bigint not null",
                "message_id bigint not null",
                "corresponds_params boolean not null",
                "complete boolean not null",
                "date timestamp with time zone not null",
            ]

# Шаблон для создания таблицы дружественных каналов
FRIENDLY_CHANNELS = [
                "CHANNEL VARCHAR(255) NOT NULL",
                "USERNAME VARCHAR(255) NOT NULL",
                "CHANNEL_ID bigint NOT NULL"
            ]
