import logging
import time
import mysql.connector

DB_NAME = "scd_impl"
PASSWORD = 'dev1kEn30.'
USER = 'root'
config = {
    "host": "127.0.0.1",
    "user": USER,
    "password": PASSWORD,
    "database": DB_NAME,
}

# Set up logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s")

# Log to console
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logger.addHandler(handler)

# Also log to a file
file_handler = logging.FileHandler("cpy-errors.log")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


def connect_to_mysql(config, attempts=3, delay=2):
    attempt = 1
    # Implement a reconnection routine
    while attempt < attempts + 1:
        try:
            return mysql.connector.connect(**config)
        except (mysql.connector.Error, IOError) as err:
            if (attempts is attempt):
                # Attempts to reconnect failed; returning None
                logger.info(
                    "Failed to connect, exiting without a connection: %s", err)
                return None
            logger.info(
                "Connection failed: %s. Retrying (%d/%d)...",
                err,
                attempt,
                attempts-1,
            )
            # progressive reconnect delay
            time.sleep(delay ** attempt)
            attempt += 1
    return None


def create_table_script():
    customer_script = ("CREATE TABLE IF NOT EXISTS `customer` ("
                       "  `id` BIGINT(255) NOT NULL AUTO_INCREMENT,"
                       "  `customerid` BIGINT(255) NOT NULL,"
                       "  `genre` varchar(14) DEFAULT NULL,"
                       "  `age` int DEFAULT NULL,"
                       "  `annual_income_(k$)` int DEFAULT NULL,"
                       "  `spending_score` int DEFAULT NULL,"
                       "  `effective_date` date NOT NULL,"
                       "  `expiration_date` date NOT NULL,"
                       "  `current_flag` bool DEFAULT false,"
                       "  PRIMARY KEY (`id`)"
                       ") ENGINE=InnoDB")
    return customer_script


def connect_to_database(cursor, cnxs):
    cursor = cursor
    cnx = cnxs
    try:
        cursor.execute("USE {}".format(DB_NAME))
    except mysql.connector.Error as err:
        print("Database {} does not exists.".format(DB_NAME))
        if err.errno == mysql.connector.errorcode.ER_BAD_DB_ERROR:
            # TODO
            # create_database(cursor)
            print("Database {} does not exists.".format(DB_NAME))
            # print("Database {} created successfully.".format(DB_NAME))
            # cnx.database = DB_NAME
        else:
            print(err)
            exit(1)


def create_table(cursor, cnx, table_name: str, table_description):
    try:
        print("Creating table {}: ".format(table_name), end='')
        print(table_description)
        cursor.execute(table_description)
    except mysql.connector.Error as err:
        if err.errno == mysql.connector.errorcode.ER_TABLE_EXISTS_ERROR:
            print("already exists.")
        else:
            print(err.msg)
    else:
        print("OK")

    cursor.close()
    cnx.close()


def conn():
    cnx = connect_to_mysql(config, attempts=3)
    if cnx and cnx.is_connected():
        print("Connected to data database")
        cur = cnx.cursor()
        connect_to_database(cur, cnx)
        create_table(cur, cnx, 'customer', create_table_script())

    else:
        print("Could not connect")
