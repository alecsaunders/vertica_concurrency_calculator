#!/usr/bin/env python3

import sys
from datetime import datetime
import argparse
import configparser as configparser
import json
import numpy as np


class ConcurrencyCalculator():

    def __init__(self, input_file, num_lines, start_position, show_preview, format_type, config=None):
        self.data = self.get_data_file_contents(input_file, num_lines, start_position)
        self.con_array = []
        self.slice_index = 0

        # Progress Bar
        self.bar_length = 50

        # Script time
        self.start = None
        self.end = None

        self.show_preview = show_preview

        self.format_type = format_type
        if self.format_type == 'text':
            self.set_output_strings(config)


    def set_output_strings(self, config=None):

        self.stats_string = (
            "Q1 Conn       : {q1}\n"
            "Median Conn   : {median}\n"
            "Q3 Conn       : {q3}\n"
            "95th % Conn   : {p95}\n"
            "98th % Conn   : {p98}\n"
            "Max Conn      : {max}\n"
            "Num of MAX    : {num_of_max}\n"
            "Average Conn  : {avg}\n"
            "Query Count   : {count}"
        )
        self.bucket_count_string = 'Count of ({0}) : {1}'
        self.script_time_string = "Script Time: {time_diff}"
        self.full_output_string = (
            "---RESULT---\n"
            "{stats}"
            "\n\n"
            "---BUCKETS---\n"
            "{buckets}"
            "\n\n"
            "{script_time}"
        )

        if config:
            if config.has_section('FORMAT_STRINGS'):
                fmt_sec = config['FORMAT_STRINGS']
                self.stats_string = fmt_sec.get('stats_string', fallback=self.stats_string)
                self.bucket_count_string = fmt_sec.get('bucket_count_string', fallback=self.bucket_count_string)
                self.script_time_string = fmt_sec.get('script_time_string', fallback=self.script_time_string)
                self.full_output_string = fmt_sec.get('full_output_string', fallback=self.full_output_string)


    @staticmethod
    def get_data_file_contents(input_file, num_lines, start_position):
        data = open(input_file, 'r').readlines()
        if num_lines:
            if start_position == 'beginning':
                data = data[:num_lines]
            if start_position == 'end':
                data = data[-num_lines:]
            if start_position == 'random' and num_lines < len(data):
                start_int_rand = np.random.randint(1, len(data) - num_lines)
                data = data[start_int_rand - 1: start_int_rand + num_lines - 1]
        return data

    def calculate(self):
        data = self.data
        data_len = len(data)
        self.start = datetime.now()

        if data_len >= 10000:
            update_interval = 100
        elif data_len >= 200:
            update_interval = 10
        else:
            update_interval = 1

        try:
            for i, query in enumerate(data, 1):
                self.cur_index = i
                concurrency = self.get_concurrency_of_query(query)
                self.con_array.append(concurrency)
                if i % update_interval == 0:
                    self.update_progress_bar(self.show_preview, self.cur_index, data_len, self.bar_length)
        except KeyboardInterrupt:
            self.cur_index = len(self.con_array)
            self.update_progress_bar(self.show_preview, self.cur_index, data_len, self.bar_length)
            print("")
            print('KeyboardInterrupt: User canceled current operation')
        finally:
            if self.show_preview:
                print('')
            self.output_results()

    @staticmethod
    def parse_query(query):
        start_epoch = query.split('|')[0].strip()
        end_epoch = query.split('|')[1].strip()
        return start_epoch, end_epoch

    def get_concurrency_of_query(self, query):
        srt_epoch, end_epoch = self.parse_query(query)
        return self.parse_other_queries_for_concurrency(srt_epoch, end_epoch)

    def parse_other_queries_for_concurrency(self, srt_epoch, end_epoch):
        concurrency = 0
        self.set_slice_index(srt_epoch)
        sliced_data = self.data[self.slice_index:self.cur_index]
        for j, other_query in enumerate(sliced_data):
            other_srt_epoch, other_end_epoch = self.parse_query(other_query)
            if other_srt_epoch < srt_epoch and other_end_epoch > srt_epoch:
                concurrency = concurrency + 1
            if other_srt_epoch > srt_epoch:
                break
        return concurrency

    def set_slice_index(self, srt_epoch):
        tmp_slice_index = self.cur_index - 100 if self.cur_index >= 100 else 0
        slice_end_epoch = self.data[tmp_slice_index].split('|')[1].strip()
        if slice_end_epoch < srt_epoch:
            self.slice_index = tmp_slice_index

    @staticmethod
    def update_progress_bar(show_preview, cur_index, data_len, bar_length):
        if show_preview:
            progress = int(float(cur_index) / float(data_len) * bar_length)
            text = "\rProgress: [{0}] {1}% {2}/{3}".format(
                "#" * progress + "-" * (bar_length - progress),
                int(float(progress) / float(bar_length) * 100),
                cur_index,
                data_len
            )
            sys.stdout.write(text)
            sys.stdout.flush()

    def output_results(self):
        self.end = datetime.now()
        npArr = np.array(self.con_array)
        max = npArr.max()

        output_stats = {}
        output_stats['max'] = int(max)
        output_stats['num_of_max'] = int((npArr == max).sum())
        output_stats['q1'] = int(np.percentile(npArr, 25))
        output_stats['median'] = int(np.percentile(npArr, 25))
        output_stats['q3'] = int(np.percentile(npArr, 25))
        output_stats['p95'] = int(np.percentile(npArr, 95))
        output_stats['p98'] = int(np.percentile(npArr, 98))
        output_stats['avg'] = float(np.average(npArr))
        output_stats['count'] = len(npArr)

        buckets = []
        for i in range(0, output_stats['max'] + 1):
            bucket_count = int((npArr == i).sum())
            buckets.append((i, bucket_count))

        output_stats['buckets'] = buckets
        output_stats['time_diff'] = self.end - self.start
        self.output_stats = output_stats

        if self.format_type == 'text':
            self.print_resutls(output_stats)
        elif self.format_type == 'json':
            output_stats['time_diff'] = str(output_stats['time_diff'])
            json_str = json.dumps(output_stats)
            print(json_str)

    def print_resutls(self, output_stats):
        stats_string = self.stats_string.format(**output_stats)
        buckets_string = "\n".join(self.buckets_string_gen(output_stats['buckets']))
        script_time_string = self.script_time_string.format(**output_stats)

        full_output_dict = {}
        full_output_dict['stats'] = stats_string
        full_output_dict['buckets'] = buckets_string
        full_output_dict['script_time'] = script_time_string
        full_output_string = self.full_output_string.format(**full_output_dict)

        print(full_output_string)

    def buckets_string_gen(self, buckets):
        for bucket in buckets:
            if len(str(bucket[0])) == 1:
                i_str = '0' + str(bucket[0])
            else:
                i_str = str(str(bucket[0]))
            yield self.bucket_count_string.format(*bucket)



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Script to get concurrency stats for Vertica queries')
    parser.add_argument('-f', '--input_file', metavar='',
                        help='Input file to parse for concurrency stats (default: csv/output.csv)')
    parser.add_argument('-n', '--num-lines', metavar='', type=int, help='Number the output lines, starting at 1')
    parser.add_argument('-s', '--start-position', metavar='', choices=['beginning', 'end', 'random'],
                        help='Position of the file to start reading files from. Options are: beginning, end, random (default: end)')
    parser.add_argument('-t', '--format-type', metavar='', help='Output format type: text or json (default: text)')
    parser.add_argument('-x', '--no-preview', action='store_true', help='Do not show live preview in console')
    parser.add_argument('-c', '--config', metavar='', help="Path to config file (default: ./config.ini)")
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

    show_preview = True if not args.no_preview else False

    config_file = args.config or 'config.ini'
    config = configparser.ConfigParser()
    config.read(config_file)

    if args.format_type:
        format_type = args.format_type
    else:
        format_type = config.get("FORMAT_TYPE", "type", fallback="text")

    cc = ConcurrencyCalculator(input_file_arg, num_lines_arg, start_position_arg, show_preview, format_type, config)
    cc.calculate()
