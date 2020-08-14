import argparse
import csv
from statistics import mean, stdev
from collections import defaultdict

parser = argparse.ArgumentParser(description='Process some integers.')

parser.add_argument('--test', default='test.tsv', help='test dataset path')
parser.add_argument('--train', default='train.tsv', help='train dataset path')


def read_data(path):
    with open(path) as f:
        reader = csv.reader(f, delimiter='\t')
        data = [x for x in reader]
    del data[0]
    features = {}

    for elem in data:
        job_id = elem[0]

        raw = elem[1].split(',')
        job = {}
        for col_num in range(len(raw)):
            if col_num == 0:
                feature_type = raw[col_num]
                continue
            job[f'feature_{feature_type}_{col_num}'] = int(raw[col_num])

        features[job_id] = job

    return features


def get_data_stats(data):
    columns = defaultdict(list)
    for v in data.values():
        for key, value in v.items():
            columns[key].append(value)
    stats = {}

    for key, value in columns.items():
        m = mean(value)
        s = stdev(value)
        max_value = max(value)
        elem = {'mean': m, 'stdev': s, 'max': max_value}
        stats[key] = elem

    return stats


def z_score(value, stat):
    return (value - stat['mean']) / stat['stdev']


def get_max_feature_index(values_list):
    max_value = max(values_list.values())
    for k, v in values_list.items():
        if v == max_value:
            return k.split('_')[-1], k


def abs_mean_diff(values, max_feature_index, stats):
    return abs(values[max_feature_index] - stats[max_feature_index]['mean'])


def process_test(data, stats):
    result = {}

    for k, v in data.items():
        result[k] = {}
        for key, value in v.items():
            _, feature_type, feature = key.split('_')


            result[k][f'feature_{feature_type}_stand_{feature}'] = z_score(value, stats[key])
            max_feature_index, max_feature_field = get_max_feature_index(v)
            result[k][f'max_feature_{feature_type}_index'] = max_feature_index
            result[k][f'max_feature_{feature_type}_abs_mean_diff'] = abs_mean_diff(v, max_feature_field, stats)

    return result


def save_proc_file(output):
    with open('test_proc.tsv', 'w', newline='') as f:
        writer = csv.writer(f, delimiter='\t', quoting=csv.QUOTE_ALL)
        header = ['id_job'] + list(list(output.values())[0].keys())
        writer.writerow(header)
        for key, value in output.items():
            line = [key]
            for column in header:
                if column == 'id_job':
                    continue
                line.append(value[column])
            writer.writerow(line)


if __name__ == "__main__":
    args = parser.parse_args()
    print(args)
    print('-' * 32)

    data = read_data(args.train)
    stats = get_data_stats(data)

    test = read_data(args.test)

    res = process_test(test, stats)
    save_proc_file(res)
    print("Complete")
