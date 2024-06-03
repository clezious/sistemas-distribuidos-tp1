import difflib
import csv

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


def main():
    compare_files('datasets/query_1.csv', 'kaggle_results/query_1_kaggle.csv')
    compare_files('datasets/query_2.csv', 'kaggle_results/query_2_kaggle.csv')
    compare_files('datasets/query_3.csv', 'kaggle_results/query_3_kaggle.csv')
    compare_files('datasets/query_4.csv', 'kaggle_results/query_4_kaggle.csv')
    compare_files_with_rounding(
        'datasets/query_5.csv', 'kaggle_results/query_5_kaggle.csv')


if __name__ == '__main__':
    main()
