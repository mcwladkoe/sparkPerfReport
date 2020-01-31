import xlsxwriter
import string


from .constants import WRITER_SETTINGS


class ResultFileWriter:
    def __init__(self, output_path, data):
        self.workbook = xlsxwriter.Workbook(output_path)
        self.data = data

    def write_results(self):

        def write_worksheet_header(worksheet):
            merged_cells = {
                'C1:E1': 'Тестування',
                'F1:H1': 'Навчання',
                'A1:A2': settings.get('first_column_title') or '№',
                'B1:B2': 'Id',
                'I1:I2': 'Серед. зн. тест. / серед. зн. навч.',
            }
            for k, v in merged_cells.items():
                worksheet.merge_range(k, v)

            nested_header_cells = {
                'C2': 'Серед.зн., с',
                'D2': 'Медіана, с',
                'E2': 'СКВ',
                'F2': 'Серед.зн., с',
                'G2': 'Медіана, с',
                'H2': 'СКВ',
            }
            for k, v in nested_header_cells.items():
                worksheet.write(k, v)

        for testpack, settings in WRITER_SETTINGS.items():
            worksheet = self.workbook.add_worksheet(
                settings.get('worksheet_name') or testpack
            )
            write_worksheet_header(worksheet, settings)

            keys = settings.get('keys') \
                or self.data[testpack].keys()
            prefix = settings['prefix']
            for index, i in enumerate(keys, 3):
                row_data = [
                    i,
                    '{}{}'.format(prefix, index + 1),
                ]
                for metric_type in ['test_time', 'training_time']:
                    for metric_param in ['mean', 'median', 'stdev']:
                        try:
                            val = round(
                                self.data[testpack][i]
                                [metric_type][metric_param], 4
                            )
                        except KeyError:
                            val = '-'
                        row_data.append(val)

                if self.data[testpack][i]['training_time']['mean']:
                    val = round(
                        self.data[testpack][i]['test_time']['mean'] /
                        self.data[testpack][i]['training_time']['mean'],
                        4
                    )
                else:
                    val = '-'
                row_data.append(val)

                for val_index, val in enumerate(row_data):
                    worksheet.write(
                        '{}{}'.format(
                            string.uppercase_ascii[val_index],
                            index
                        ),
                        val
                    )

        self.workbook.close()
