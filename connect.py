import logging
import time
import mysql.connector

config = {
    "host": "127.0.0.1",
    "user": "root",
    "password": "123456",
    "database": "scd_impl",
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


def create_table():
    customer_script = "CREATE TABLE IF NOT EXISTS `customer` ("
    "  `id` long NOT NULL AUTO_INCREMENT,"
    "  `customer_id` long NOT NULL,"
    "  `genre` varchar(14) DEFAULT NULL,"
    "  `age` int DEFAULT NULL,"
    "  `annual_income_(k$)` int DEFAULT NULL,"
    "  `spending_score` date NOT NULL,"
    "  PRIMARY KEY (`emp_no`)"
    ") ENGINE=InnoDB"


def conn():
    cnx = connect_to_mysql(config, attempts=3)

    if cnx and cnx.is_connected():
        print("Connected to data database")
    else:
        print("Could not connect")
