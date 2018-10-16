#!/usr/bin/env python3

import sys
from datetime import datetime
import argparse
import numpy as np


class ConcurrencyCalculator():

    def __init__(self, input_file, num_lines, start_position):
        self.data = self.get_data_file_contents(input_file, num_lines, start_position)

        self.max_end_epoch = float(0.0)
        self.con_array = []

        # Progress Bar
        self.data_len = 0
        self.bar_length = 50

        # Script time
        self.start = None
        self.end = None

    @staticmethod
    def get_data_file_contents(input_file, num_lines, start_position):
        if num_lines:
            if start_position == 'beginning':
                data = open(input_file, 'r').readlines()[:num_lines]
            if start_position == 'end':
                data = open(input_file, 'r').readlines()[-num_lines:]
            if start_position == 'random':
                data_file = open(input_file, 'r').readlines()
                start_int_rand = np.random.randint(1, len(data_file) - num_lines)
                data = data_file[start_int_rand - 1: start_int_rand + num_lines - 1]
        else:
            data = open(input_file, 'r').readlines()
        return data

    def calculate(self):
        data = self.data
        self.data_len = len(data)

        self.start = datetime.now()
        try:
            for i, r in enumerate(data, 1):
                query = r.strip().split('|')
                start_epoch = float(query[0])
                end_epoch = float(query[1])
                req_con = 0

                slice_index = self.get_slice_index(data, i, start_epoch)
                end_index = self.get_end_index(i, end_epoch)

                for j in data[slice_index:end_index]:
                    jreq = j.strip().split('|')
                    comp_start = float(jreq[0])
                    comp_end = float(jreq[1])

                    if comp_end > self.max_end_epoch:
                        self.max_end_epoch = comp_end

                    if comp_start < start_epoch and comp_end > start_epoch:
                        req_con = req_con + 1
                    if comp_start > end_epoch:
                        break
                self.con_array.append(req_con)

                # Update the progress bar
                self.update_progress_bar(i)
        except KeyboardInterrupt:
            print("")
            print("KeyboardInterrupt")
            print("- Printing all data up to this point")
        except Exception as e:
            print(query)
            print(j)
            raise e

        self.print_results()

    def get_slice_index(self, data, index, cur_start_epoch):
        i = index
        if i > 1000:
            slice_index = i - 1000

            if cur_start_epoch > self.max_end_epoch:
                slice_index = i
            else:
                try:
                    while float(data[slice_index].strip().split('|')[1]) > cur_start_epoch:
                        if slice_index > 100:
                            slice_index = 0
                            break
                        else:
                            slice_index = slice_index - 100
                except Exception as e:
                    print("")
                    print("Unhandled exception occurred")
                    print(e)
                    print("Error occurred at indices")
                    print(i)
                    print("")
                    print("- Printing all data up to this point")
                    self.print_results()
                    sys.exit(1)
        else:
            slice_index = 0
        return slice_index

    def get_end_index(self, i, end_epoch):
        end_index = i + 2
        tmp_index = i + end_index
        if tmp_index >= len(self.data):
            end_index = len(self.data) + 1
        else:
            data_line = self.data[tmp_index]
            tmp_start_epoch = float(data_line.split('|')[0].strip())
            while end_epoch > tmp_start_epoch:
                end_index = end_index + 100
                if end_index >= len(self.data):
                    return len(self.data) + 1
        return end_index

    def update_progress_bar(self, index):
        progress = int(float(index) / float(self.data_len) * self.bar_length)
        text = "\rProgress: [{0}] {1}% {2}".format("#" * progress + "-" * (self.bar_length - progress),
                                                   int(float(progress) / float(self.bar_length) * 100),
                                                   "{0}/{1}".format(index, self.data_len))
        sys.stdout.write(text)
        sys.stdout.flush()

    def print_results(self):
        self.end = datetime.now()
        npArr = np.array(self.con_array)

        max = npArr.max()
        num_of_max = (npArr == max).sum()

        print("")
        print("---RESULT---")
        print("Q1 Conn       : {}".format(np.percentile(npArr, 25)))
        print("Median Conn   : {}".format(np.percentile(npArr, 50)))
        print("Q3 Conn       : {}".format(np.percentile(npArr, 75)))
        print("95th % Conn   : {}".format(np.percentile(npArr, 95)))
        print("98th % Conn   : {}".format(np.percentile(npArr, 98)))
        print("Max Conn      : {}".format(max))
        print("Num of MAX    : {}".format(num_of_max))
        print("Avgerage Conn : {}".format(np.average(npArr)))
        print("Query Count   : {}".format(len(npArr)))
        print("")

        print("---BUCKETS---")
        for i in range(0, max + 1):
            bucket_num = (npArr == i).sum()
            if len(str(i)) == 1:
                i_str = '0' + str(i)
            else:
                i_str = str(i)
            print('Count of ({0}) : {1}'.format(i_str, bucket_num))

        time_diff = self.end - self.start
        print("\nScript Time: " + str(time_diff))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Script to get concurrency stats for Vertica queries')
    parser.add_argument('-f', '--input_file', metavar='',
                        help='Input file to parse for concurrency stats (default: csv/output.csv)')
    parser.add_argument('-n', '--num_lines', metavar='', type=int, help='Number the output lines, starting at 1')
    parser.add_argument('-s', '--start_position', metavar='', choices=['beginning', 'end', 'random'],
                        help='Position of the file to start reading files from. Options are: beginning, end, random')
    args = parser.parse_args()

    input_file_arg = args.input_file or 'csv/output.csv'
    num_lines_arg = args.num_lines
    if num_lines_arg:
        start_position_arg = args.start_position or 'end'
    else:
        if args.start_position:
            print("Number of lines not specified. Use -n option to specify number of lines to read.")
            sys.exit(1)
        else:
            start_position_arg = None

    cc = ConcurrencyCalculator(input_file_arg, num_lines_arg, start_position_arg)
    cc.calculate()
