import pymysql
import json

with open('../config/config.json') as f:
    config = json.load(f)


def connect_db():
    conn = pymysql.connect(
        host=config['MYSQL_HOST'],
        user=config['MYSQL_USER'],
        password=config['MYSQL_PASSWORD'],
        db=config['MYSQL_DB'],
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor
    )
    return conn


def execute_query(conn, query, *args):
    with conn.cursor() as cursor:
        cursor.execute(query, args)
        result = cursor.fetchall()
    return result


def execute_insert_query(conn, query):
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()


def drop_tables_stock_issues(conn):
    drop_table_query = """
    DROP TABLE IF EXISTS stock_issues;
    """
    execute_query(conn, drop_table_query)
    print(f"stock_issues table is DROPPED!")


def create_table_stock_issues(conn):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS stock_issues (
        report_time DATETIME NOT NULL,
        title VARCHAR(400) NOT NULL,
        stock_name VARCHAR(255) NOT NULL,        
        news_content TEXT,
        news_link VARCHAR(600),
        channel_name VARCHAR(255),
        PRIMARY KEY (report_time, title, stock_name)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """
    execute_query(conn, create_table_query)
