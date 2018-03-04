#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import io
import csv
import sys
import argparse
import datetime
from math import isclose

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


def output2(title, users):
    # Write

    with io.open(OUT_FILE_NAME, 'a', encoding='utf8') as txtfile:
        txtfile.write("\n%s\n" % title)

        i = 0
        for item in users:
            i += 1
            if not item:
                u = "NOT_SPECIFIED"
            formatted_string = "{}. User {:32} sends request periodically\n"
            txtfile.write(formatted_string.format(i, item))


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


def aggregate_by_user_periods(csv_filename, field):
    # Read and process
    users = {}
    with open(csv_filename, 'rt') as csv_file:
        try:
            rows = csv.DictReader(csv_file)
            next(rows)

            # Dict of users with time series list
            for row in rows:
                if row[field] not in users:
                    users[row[field]] = []
                users[row[field]].append(row['_time'])

            users_time_series = {}
            for user in users.keys():

                # Excluding users sending 1 requests only
                if len(users[user]) == 1:
                    continue

                # Sort each user's time series
                sorted_series = []
                for t in users[user]:
                    sorted_series.append(datetime.datetime.strptime(t[:19], "%Y-%m-%dT%H:%M:%S"))
                users_time_series[user] = sorted(sorted_series)

                i = 0
                delta = []
                # print(user)
                # Get time diff between each following time series, in sec, exclude requests sent at the same time
                while i < len(users[user]) - 1:
                    diff = int((users_time_series[user][i+1] - users_time_series[user][i]).total_seconds())
                    if diff:
                        delta.append(diff)
                    i += 1
                users_time_series[user] = delta

            users_periods = []

            for user in users_time_series.keys():
                # Exclude users with 1 difference (Users sent 2 requests are excluded as well)
                if not users_time_series[user] or len(users_time_series[user]) == 1:
                    continue
                i = 0

                # Calculate how close timeseries to each other
                # if difference is similar, consider it as repeated period, precision is +/-100 seconds
                periods = []

                # Assume user sends periodically
                flag = 1
                while i < len(users_time_series[user]) - 1:
                    similar = isclose(users_time_series[user][i], users_time_series[user][i+1], abs_tol=100)
                    periods.append(similar)
                    i += 1
                    # If time diff between requests is not similar, assume it is not periodical
                    if not similar:
                        flag = 0
                if flag:
                    users_periods.append(user)
            return users_periods

        except IOError:
            print("Was not able to process file {}, exiting".format(IN_FILE_NAME))
            sys.exit()


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
        # Task 3
        output2(
            "# Задание 3: Поиск регулярных запросов (запросов выполняющихся периодически) по полю src_user.",
            aggregate_by_user_periods(file, 'src_user'),
        )
        # Task 4
        output2(
            "# Задание 4: Поиск регулярных запросов (запросов выполняющихся периодически) по полю src_ip.",
            aggregate_by_user_periods(file, 'src_ip'),
        )
