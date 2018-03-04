from os import listdir

import pandas as pd


def only_digit(s):
    return ''.join(x for x in s if x.isdigit())


if __name__ == "__main__":
    frame = pd.read_csv('metadata/full.csv', delimiter=';')

    print('length full: ', len(frame))

    frame = frame[frame['Type'] == 'sfw']

    print('length sfw: ', len(frame))

    frame = frame.drop_duplicates()

    print('length dedup: ', len(frame))

    frame = frame.groupby(by='Image').agg({'Name': lambda x: ','.join(x)})

    frame = frame.reset_index()

    print('length wallpapers: ', len(frame))

    frame['Image'].to_csv('metadata/wallpapers.txt', index=False)

    existent_wallapers = listdir('images')
    existent_wallapers = [only_digit(x) for x in existent_wallapers]
    existent_wallapers = set(existent_wallapers)

    frame = frame[frame['Image'].isin(existent_wallapers)]

    frame.to_csv('metadata/ready.csv', index=False)

    print(frame.head())
