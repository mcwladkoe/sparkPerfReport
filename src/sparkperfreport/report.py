import os
import sys
import argparse

import statistics
import transaction

import json

from collections import defaultdict

from sqlalchemy import create_engine
from sqlalchemy.orm import joinedload

from . import DBSession
from .constants import (
    IGNORE_TESTS,
)

from .models import (
    SparkPerfTestingResults,
    SparkPerfTestResultTypeEnum,
    SparkPerfClusterTest,
    SparkPerfTestPack,
)

from .writer import ResultFileWriter


class MetricsData:
    def __init__(self, cluster_label):
        self.data = defaultdict(dict)
        self.cluster_label = cluster_label

    def get_data_from_folder(self, input_path):
        self.input_path = input_path
        file_list = os.listdir(self.input_path)
        for folder in file_list:
            path = os.path.join(self.input_path, folder)
            if not os.path.isdir(path):
                continue
            test_name = folder.split('_')[0]
            self.data[test_name] = defaultdict(dict)
            for file in os.listdir(path):
                name, ext = os.path.splitext(file)
                if name in IGNORE_TESTS:
                    continue
                if ext == '.out':
                    self.get_data_from_file(
                        test_name, name, os.path.join(path, file)
                    )

    def get_data_from_file(self, test_name, name, file_path):
        with open(file_path, 'r') as f:
            lines = f.readlines()
            dt_num = 1

            counts = defaultdict(int)
            for i in lines[4:]:
                splitted = i.split(' ')
                if splitted[0] != 'results:':
                    continue
                d = json.loads(' '.join(splitted[1:]))
                try:
                    res = SparkPerfTestingResults(d['results'])
                    dct = {}
                    for i in SparkPerfTestResultTypeEnum.names():
                        stat = {}
                        for func in ['stdev', 'mean', 'median']:
                            try:
                                stat[func] = getattr(statistics, func)(
                                    getattr(res, i)
                                )
                            except (TypeError, ValueError):
                                stat[func] = None
                        dct[i] = stat
                    if d['testName'] == 'decision-tree':
                        self.data['decision-tree'][str(dt_num)] = dct
                        dt_num += 1
                    else:
                        counts[d['testName']] += 1
                        if counts[d['testName']] > 1:
                            if self.data[test_name].get(d['testName']):
                                self.data[test_name][d['testName'] + '-1'] = \
                                    self.data['mllib'][d['testName']]
                                del self.data['mllib'][d['testName']]
                            self.data[test_name]['{}-{}'.format(
                                d['testName'], counts[d['testName']]
                            )] = dct
                        else:
                            self.data[test_name][d['testName']] = dct
                except KeyError:
                    pass

    def write_results_to_db(self):
        cluster = SparkPerfClusterTest(cluster_label=self.cluster_label)
        DBSession.add(cluster)
        DBSession.flush()

        cluster.process_data(self.data)

    def get_data_from_db(self):
        cluster = DBSession.query(SparkPerfClusterTest) \
            .filter(SparkPerfClusterTest.cluster_label == self.cluster_label) \
            .options(
                joinedload(SparkPerfClusterTest.test_packs_data)
                .joinedload(SparkPerfTestPack.results)
            ) \
            .first()
        if cluster:
            print(cluster.to_dict())

    def write_results_to_file(self, output_path):
        writer = ResultFileWriter(output_path, self.data)
        writer.write_results()


def write_results_to_file_from_data(data, output_path, cluster):
    pass


def main(argv=sys.argv):
    description = """
        Get report for SparkPerf metrics in XLS format.
    """
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        'cluster_label',
        metavar='cluster_label',
        help='Cluster label (for DB)'
    )

    parser.add_argument(
        'database_url',
        metavar='database_url',
        help='Database URL'
    )

    parser.add_argument(
        '-i', '--input-path',
        dest='input_path',
        help='Path to results folder'
    )

    parser.add_argument(
        '-o', '--output', dest='output_path',
        default='results.xlsx',
        help='Output file path'
    )

    parser.add_argument(
        '-w', '--write-file',
        dest='write_file',
        action='store_true',
        help='Write result in file'
    )

    parser.add_argument(
        '-r', '--read-directory',
        dest='read_directory',
        action='store_true',
        help='Read results from directory'
    )

    args = parser.parse_args(argv[1:])

    engine = create_engine(args.database_url)

    DBSession.configure(bind=engine)

    data = MetricsData(args.cluster_label)

    data.get_data_from_db()

    if args.read_directory and not args.input_path:
        raise RuntimeError('Please select input_path.')

    elif args.read_directory:
        data.get_data_from_folder(args.input_path)
        if not data.data:
            print('Input data folder is empty or no data found.')
            return
        with transaction.manager:
            data.write_results_to_db()

    if args.write_file:
        # TODO: add read data from DB
        data.write_results_to_file()


if __name__ == '__main__':
    main()
