import logging
import time
import mysql.connector

DB_NAME = 'scd_impl'
PASSWORD = 'dev1kEn30.'
USER = 'root'
HOST = '127.0.0.1',
config = {
    "host": HOST[0],
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
                       "  `customerid` BIGINT(255) NOT NULL,"
                       "  `sk_customerid` BIGINT(255) NOT NULL,"
                       "  `genre` varchar(14) DEFAULT NULL,"
                       "  `age` int DEFAULT NULL,"
                       "  `annual_income_(k$)` int DEFAULT NULL,"
                       "  `spending_score` int DEFAULT NULL,"
                       "  `effective_date` date NOT NULL,"
                       "  `expiration_date` date NOT NULL,"
                       "  `current_flag` bool DEFAULT false,"
                       "  PRIMARY KEY (`customerid`, `sk_customerid`)"
                       ") ENGINE=InnoDB")
    return customer_script


def connect_to_database(cursor):
    cursor = cursor
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
    # cnx.close()


def check_table_existed(cursor, table_name, schema_name):
    sql_script = f"""SELECT EXISTS (
    SELECT * FROM information_schema.tables
    WHERE table_schema = '{schema_name}'
    AND table_name = '{table_name}'
    )"""
    cursor.execute(sql_script)
    result = cursor.fetchone()
    if result and result[0] == 1:
        return True
    return False


def conn():
    cnx = connect_to_mysql(config, attempts=3)
    if cnx and cnx.is_connected():
        print("Connected to data database")
        cur = cnx.cursor()
        connect_to_database(cur)
        if check_table_existed(cur, 'customer', DB_NAME) is False:
            create_table(cur, cnx, 'customer', create_table_script())
        cur.close()
        cnx.close()
    else:
        print("Could not connect")


def process_many_rows(df, table_name, change_type):
    rows = df.rdd.collect()
    sql_string = ''
    lst_params = ''
    # add apostrophes to wrapped a field name
    list_fields = (', '.join('`' + item + '`' for item in rows[0].__fields__))
    table_pk_id = "{tableName}id".format(tableName=table_name)
    table_sk_id = "sk_{tableName}id".format(tableName=table_name)
    for i in rows[0].__fields__:
        if change_type == 'INSERT':
            lst_params = lst_params + '%s,'
        elif change_type == 'UPDATE':
            if i == table_pk_id or i == table_sk_id:
                continue
            lst_params = lst_params + "{field} = %s,".format(field=i)
    # Remove the last comma "," of the string
    lst_params = lst_params[:-1]
    if change_type == 'INSERT':
        sql_string = f"""INSERT INTO {table_name} ({list_fields}) VALUES ({lst_params}) """
    elif change_type == 'UPDATE':
        sql_string = f"""UPDATE {table_name} SET {lst_params} WHERE {table_pk_id} = %s AND {table_sk_id} = %s"""
    print(sql_string)
    lst_rows = []
    for row in rows:
        if change_type == 'INSERT':
            value = ()
            for field in row.__fields__:
                value = value + (row[field],)
            lst_rows.append(value)
        elif change_type == 'UPDATE':
            value = ()
            for field in row.__fields__:
                if field == table_pk_id or field == table_sk_id:
                    continue
                value = value + (row[field],)
            # Add pk and sk fields the last for the order of command
            value = value + (row[table_pk_id], row[table_sk_id])
            lst_rows.append(value)
    print(lst_rows)
    if change_type == 'INSERT':
        insert_multiple_rows(insert_query=sql_string,
                             records_to_insert=lst_rows, table_name=table_name)
    elif change_type == 'UPDATE':
        update_mutiple_rows(update_query=sql_string,
                            records_to_update=lst_rows, table_name=table_name)


def insert_multiple_rows(insert_query, records_to_insert, table_name):
    try:
        cnx = connect_to_mysql(config, attempts=3)
        cur = cnx.cursor()
        #   cursor = connection.cursor()
        cur.executemany(insert_query, records_to_insert)
        cnx.commit()
        print(cur.rowcount, "Record inserted successfully into {table_name} table".format(
            table_name=table_name))
    except mysql.connector.Error as error:
        print("Failed to insert record into MySQL table {}".format(error))
    finally:
        if cnx.is_connected():
            cur.close()
            cnx.close()
            print("MySQL connection is closed")


def update_mutiple_rows(update_query, records_to_update, table_name):
    try:
        cnx = connect_to_mysql(config, attempts=3)
        cur = cnx.cursor()
        cur.executemany(update_query, records_to_update)
        cnx.commit()

        print(cur.rowcount, "Records of a {table_name} table updated successfully".format(
            table_name=table_name))

    except mysql.connector.Error as error:
        print("Failed to update records to database: {}".format(error))
    finally:
        if cnx.is_connected():
            cur.close()
            cnx.close()
            print("MySQL connection is closed")
