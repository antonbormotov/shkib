#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import io
import csv
import sys
import argparse

IN_FILE_NAME = 'test.csv'
OUT_FILE_NAME = 'result.txt'
TOP_USERS = 5


def get_input_file():
    parser = argparse.ArgumentParser()
    parser.add_argument('--file',
                        nargs='?',
                        default=IN_FILE_NAME
                        )
    args = parser.parse_args()
    return args.file


def output(title, users, sorting_type):
    # Write
    list_users = sorted(users.items(), key=lambda m: (m[1][sorting_type]), reverse=True)
    with io.open(OUT_FILE_NAME, 'a', encoding='utf8') as txtfile:
        txtfile.write("\n%s\n" % title)

        if sorting_type == 'req':
            formatted_string = "{}. User {:32} has generated {} requests\n"
        else:
            formatted_string = "{}. User {:32} has sent {} bytes\n"

        i = 0
        for item in list_users[:TOP_USERS]:
            i += 1
            u = item[0]
            if not item[0]:
                u = "NOT_SPECIFIED"
            txtfile.write(formatted_string.format(i, u, item[1][sorting_type]))


def aggregate_by_user(csv_filename):
    # Read and process
    users_dict = {}
    with open(csv_filename, 'rt') as csv_file:
        try:
            rows = csv.DictReader(csv_file)
            next(rows)
            for row in rows:
                if row['src_user'] not in users_dict:
                    users_dict[row['src_user']] = {'req': 0, 'byte': 0}
                users_dict[row['src_user']]['req'] += 1
                users_dict[row['src_user']]['byte'] += int(row['output_byte'])
        except IOError:
            print("Was not able to process file {}, exiting".format(IN_FILE_NAME))
            sys.exit()
    return users_dict


if __name__ == "__main__":

        # Check if input file exists
        file = get_input_file()
        if not os.path.exists(file):
            print("Was not able to open file, exiting".format(file))
            sys.exit()

        # Proceed to processing
        io.open(OUT_FILE_NAME, 'w', encoding='utf8').truncate()
        users = aggregate_by_user(
            file
        )
        # Task 1
        output(
            "# Поиск 5ти пользователей, сгенерировавших наибольшее количество запросов",
            users,
            'req'
        )
        # Task 2
        output(
            "# Поиск 5ти пользователей, отправивших наибольшее количество данных",
            users,
            'byte'
        )
