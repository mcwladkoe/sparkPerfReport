import os
import sys
import argparse

import json

from collections import defaultdict
import xlsxwriter

from .constants import (
    MLLIB_TESTS,
    IGNORE_TESTS,
)

import statistics

from .model import SparkPerfTestingResults


class MetricsData:
    def __init__(self, input_path):
        self.data = defaultdict(dict)
        self.input_path = input_path

    def get_data_from_folder(self):
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
                        test_name,
                        name,
                        os.path.join(
                            path,
                            file
                        ))

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
                    for i in ['training_time', 'test_time']:
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
                        self.data['decision_tree'][str(dt_num)] = dct
                        dt_num += 1
                    else:
                        counts[d['testName']] += 1
                        if counts[d['testName']] > 1:
                            if self.data[test_name].get(d['testName']):
                                self.data[test_name][d['testName'] + '-1'] = \
                                    self.data['mllib'][d['testName']]
                                del self.data['mllib'][d['testName']]
                            self.data[test_name]['{}-{}'.format(
                                d['testName'],
                                counts[d['testName']]
                            )] = dct
                        else:
                            self.data[test_name][d['testName']] = dct
                except KeyError:
                    pass


def write_results(data, output_path):
    workbook = xlsxwriter.Workbook(output_path)

    for testpack in ['decision_tree', 'mllib']:
        worksheet = workbook.add_worksheet(testpack)
        worksheet.merge_range('C1:E1', 'Тестування')
        worksheet.merge_range('F1:H1', 'Навчання')
        f_column_name = '№' if testpack == 'decision_tree' else 'Назва тесту'
        worksheet.merge_range(
            'A1:A2',
            f_column_name
        )
        worksheet.merge_range('B1:B2', 'Id')
        worksheet.merge_range('I1:I2', 'Серед. зн. тест. / серед. зн. навч.')
        worksheet.write('C2', 'Серед.зн., с')
        worksheet.write('D2', 'Медіана, с')
        worksheet.write('E2', 'СКВ')
        worksheet.write('F2', 'Серед.зн., с')
        worksheet.write('G2', 'Медіана, с')
        worksheet.write('H2', 'СКВ')
        worksheet.write('J1', 'Відношення тест/навч')
        keys = data[testpack].keys() if testpack == 'decision_tree' else MLLIB_TESTS
        prefix = 'DTR' if testpack == 'decision_tree' else 'ML'
        for index, i in enumerate(keys):
            worksheet.write('A{}'.format(index + 3), i)
            worksheet.write(
                'B{}'.format(index + 3),
                '{}{}'.format(prefix, index + 1)
            )
            try:
                worksheet.write(
                    'C{}'.format(index + 3),
                    round(data[testpack][i]['test_time']['mean'], 4)
                )
                worksheet.write(
                    'D{}'.format(index + 3),
                    round(data[testpack][i]['test_time']['median'], 4)
                )
                worksheet.write(
                    'E{}'.format(index + 3),
                    round(data[testpack][i]['test_time']['stdev'], 4)
                )
                worksheet.write(
                    'F{}'.format(index + 3),
                    round(data[testpack][i]['training_time']['mean'], 4)
                    if data[testpack][i]['training_time']['mean'] else '-'
                )
                worksheet.write(
                    'G{}'.format(index + 3),
                    round(data[testpack][i]['training_time']['median'], 4)
                    if data[testpack][i]['training_time']['median'] else '-'
                )
                worksheet.write(
                    'H{}'.format(index + 3),
                    round(data[testpack][i]['training_time']['stdev'], 4)
                    if data[testpack][i]['training_time']['stdev'] else '-'
                )
                if data[testpack][i]['training_time']['mean']:
                    worksheet.write('I{}'.format(index + 3), round(
                        data[testpack][i]['test_time']['mean'] /
                        data[testpack][i]['training_time']['mean'], 4))

                worksheet.write(
                    'J{}'.format(index + 3),
                    '=I{}/J2'.format(index + 3)
                )
            except KeyError as e:
                print(e)
                pass
        worksheet.write(
            'J2',
            '=MAX(I:I)'
        )
        data_length = len(data[testpack]) + 3
        chart = workbook.add_chart({'type': 'column'})
        chart.add_series({
            'values': '={sheet}!$J$3:$J${length}'.format(
                sheet=testpack,
                length=data_length
            ),
            'categories': '={sheet}!$A$3:$A${length}'.format(
                sheet=testpack,
                length=data_length
            ),
            'name': 'Відношення тест/навч',
        })
        chart.set_x_axis({'name': f_column_name, 'rotate': 90})
        chart.set_legend({'position': 'none'})
        worksheet.insert_chart(
            'J20',
            chart
        )  # chr(65) = A
    worksheet = workbook.add_worksheet('core')
    worksheet.write('A1', 'Назва тесту')
    worksheet.write('B1', 'Ідентифікатор')
    worksheet.write('C1', 'Серед.зн., с')
    worksheet.write('D1', 'Медіана, с')
    worksheet.write('E1', 'СКВ')
    keys = data['spark'].keys()
    prefix = 'C'
    for index, i in enumerate(keys):
        worksheet.write('A{}'.format(index + 2), i)
        worksheet.write(
            'B{}'.format(index + 2), '{}{}'.format(prefix, index + 1)
        )
        try:
            worksheet.write(
                'C{}'.format(index + 2),
                round(data['spark'][i]['test_time']['mean'], 4)
            )
            worksheet.write(
                'D{}'.format(index + 2),
                round(data['spark'][i]['test_time']['median'], 4)
            )
            worksheet.write(
                'E{}'.format(index + 2),
                round(data['spark'][i]['test_time']['stdev'], 4)
            )
        except KeyError as e:
            print(e)
            pass
    workbook.close()


def main(argv=sys.argv):
    description = """
        Get report for SparkPerf metrics in XLS format.
    """
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        'input_path',
        metavar='input_path',
        help='Path to results folder'
    )

    parser.add_argument(
        '-o',
        '--output',
        dest='output_path',
        default='results.xlsx',
        help='Output file path'
    )

    args = parser.parse_args(argv[1:])

    data = MetricsData(args.input_path)
    data.get_data_from_folder()

    if not data.data:
        print('Input data folder is empty or no data found.')
        return
    write_results(data.data, args.output_path)


if __name__ == '__main__':
    main()
