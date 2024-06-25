import difflib
import csv
import os
import argparse

ROUNDING_PRECISION = 5


def compare_files(file1, file2):
    diff_str = ''
    with open(file1, 'r') as f1:
        with open(file2, 'r') as f2:
            diff = difflib.context_diff(
                f1.readlines(),
                f2.readlines(),
                fromfile=file1,
                tofile=file2
            )

            diff_str = ''.join(diff)

    if diff_str:
        print(diff_str)
    else:
        print(f'Files {file1} and {file2} are identical')


def compare_files_with_rounding(file1, file2):
    with open(file1, 'r') as f1:
        with open(file2, 'r') as f2:
            reader1 = csv.reader(f1)
            reader2 = csv.reader(f2)
            for row1, row2 in zip(reader1, reader2):
                for val1, val2 in zip(row1, row2):
                    try:
                        val1 = round(float(val1), ROUNDING_PRECISION)
                        val2 = round(float(val2), ROUNDING_PRECISION)
                    except ValueError:
                        pass
                    finally:
                        if val1 != val2:
                            print(f'Files {file1} and {file2} are different')
                            print(f'Values {val1} and {val2} are different')
                            return

    print(f'Files {file1} and {file2} are identical')


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('expected_results', type=str,
                        help='Path to the expected results folder')
    parser.add_argument('results', type=str,
                        help='Path to the results folder')
    args = parser.parse_args()
    return args.expected_results, args.results


def main():
    expected_results, results = parse_args()
    results_folders = [f.path for f in os.scandir(results) if f.is_dir()]
    if not results_folders:
        print(f'No results found in {results}')
        return

    for folder in results_folders:
        print(f'Comparing results in {folder}')
        compare_files(f'{folder}/query_1.csv',
                      f'{expected_results}/query_1.csv')
        compare_files(f'{folder}/query_2.csv',
                      f'{expected_results}/query_2.csv')
        compare_files(f'{folder}/query_3.csv',
                      f'{expected_results}/query_3.csv')
        compare_files(f'{folder}/query_4.csv',
                      f'{expected_results}/query_4.csv')
        compare_files_with_rounding(
            f'{folder}/query_5.csv', f'{expected_results}/query_5.csv')
        print()


if __name__ == '__main__':
    main()
