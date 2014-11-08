__author__ = 'DeonHeyns'

import pandas as pd
import os
import glob
import sys


class CaliforniaProcessor:
    def __init__(self):
        self._data_set_directory = 'california'
        self._processed_directory = 'California-Processed'

    # noinspection PyMethodMayBeStatic
    def create_dataframe(self, path):
        data_file = open(path, 'r')
        contents = data_file.read()
        data_file.close()
        df = pd.read_html(contents)
        df = df[3]

        df1 = df[[0, 1, 2]]
        df2 = df[[4, 5, 6]]
        df1.columns = ['Company Name', 'Annual Premium', 'Deductible']
        df2.columns = ['Company Name', 'Annual Premium', 'Deductible']
        df1 = df1.drop(df1.index[0])
        df2 = df2.drop(df2.index[0])

        df = pd.concat([df1, df2])
        df = df.set_index('Company Name')
        df.sort_index(ascending=True, inplace=True)
        df.fillna('N/A')
        return df

    def process(self):
        if not os.path.exists(self.processed_directory):
            os.makedirs(self.processed_directory)

        directories = glob.glob(self.data_set_directory + '/*')

        directory_counter = 1
        for directory in directories:
            directory_split = directory.split('/')
            file_name = directory_split[len(directory_split) - 1]
            print("Processing {} number {} of {}".format(file_name, directory_counter, len(directories)))
            frames = []
            excel_file = "{}/{}.xlsx".format(self.processed_directory, file_name)
            writer = pd.ExcelWriter(excel_file)

            data_sets = glob.glob(directory + '/*')
            data_set_counter = 1
            for data_set in data_sets:
                data_set_split = data_set.split('/')
                data_set_name = data_set_split[len(data_set_split) - 1]
                print("Processing {} number {} of {}".format(data_set_name, data_set_counter, len(data_sets)))
                data_set_name = data_set_name.replace('.html', '')
                worksheet_name = data_set_name.replace(file_name + '_', '')
                worksheet_name = worksheet_name.replace('_', '')[0:31]
                df = self.create_dataframe(data_set)
                df.to_excel(writer, worksheet_name, index=True)
                frames.append(df)
                print("Processed {} number {} of {}".format(data_set_name, data_set_counter, len(data_sets)))
                data_set_counter += 1

            print("Processed {} number {} of {}".format(file_name, directory_counter, len(directories)))
            directory_counter += 1
            print("Writing {}".format(excel_file))
            writer.save()
            print("Done writing {}".format(excel_file))

    @property
    def data_set_directory(self):
        return self._data_set_directory

    @property
    def processed_directory(self):
        return self._processed_directory


def main():
    california_processor = CaliforniaProcessor()
    california_processor.process()


if __name__ == '__main__':
    sys.exit(main())