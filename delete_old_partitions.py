#!/usr/bin/env python

import psycopg2
from collections import defaultdict


def get_config():
    return {"dbname": "db", "user": "user", "password": "password", "host": "localhost"}


def get_connect(dbname, user, password, host="localhost"):
    return psycopg2.connect(dbname=dbname, user=user, password=password, host=host)


def close_connect(conn):
    if conn:
        conn.close()


def close_cursor(cursor):
    if cursor:
        cursor.close()


def get_tables():

    tables = []

    config = get_config()
    conn = get_connect(config['dbname'], config['user'], config['password'], config['host'])

    cursor = conn.cursor()

    SQL = "SELECT table_name FROM information_schema.tables where table_schema='public' order by table_name"

    try:
        cursor.execute(SQL)
        for row in cursor:
            tables.append(row[0])
    finally:
        close_cursor(cursor)
        close_connect(conn)

    return tables


def parse_tables(tables):

    temp = defaultdict(list)

    for table in tables:
        parts = table.rsplit("_", 1)
        if len(parts) == 2 and parts[1].isdigit():
            temp[parts[0]].append(int(parts[1]))

    return temp


def get_old_partitions(partitions, hold=10):
    sorted_partitions = sorted(partitions, reverse=True)
    return sorted_partitions[hold:]


def delete_partition(table, partitions):

    if not partitions:
        return

    config = get_config()
    conn = get_connect(config['dbname'], config['user'], config['password'], config['host'])

    for partition in partitions:
        table_name = "{0}_{1}".format(table, partition)
        print("deleting table: {}".format(table_name))
        template = "DROP TABLE public.{0}".format(table_name)

        cursor = conn.cursor()
        try:
            cursor.execute(template)
        except Exception as exp:
            print(exp)
        finally:
            close_cursor(cursor)

    conn.commit()

    close_connect(conn)


def main():
    tables = get_tables()
    parsed_tables = parse_tables(tables)

    for parsed_table in parsed_tables.items():
        table, partitions = parsed_table
        old_partitions = get_old_partitions(partitions)
        delete_partition(table, old_partitions)

if __name__ == '__main__':
    main()
