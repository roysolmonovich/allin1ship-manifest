import pandas as pd
from datetime import datetime

from models.manifest import ManifestRaw


def create_df(columns, dtype, headers, pf, filename, api_file_path, name):
    if pf != 'manual':
        f_ext = filename.rsplit('.', 1)[1]
        if f_ext == 'xlsx':
            df = pd.read_excel(api_file_path, usecols=columns, dtype=dtype)
        elif f_ext == 'csv':
            df = pd.read_csv(api_file_path, usecols=columns, dtype=dtype)
    else:
        print(name, columns, dtype)
        df = ManifestRaw.find_shipments_by_name(name, columns, dtype=str)
        df = df.astype(dtype)
    df = df[columns]
    df = df.rename(columns=headers)
    df.dropna(how='all', inplace=True)
    dup_df = df[df.duplicated(subset=['orderno'], keep=False)]
    duplicate_index = 0
    for i, row in dup_df.iterrows():
        df.loc[i, 'orderno'] += f'-{duplicate_index}'
        duplicate_index += 1
    df.dropna(subset=['weight'], inplace=True)
    if 'insured' in dtype and dtype['insured'] == 'float':
        df['insured'].fillna(value=0, inplace=True)
        df['insured'] = df['insured'].astype(bool)
    if 'shipdate' in df.columns:
        shipdate_is_numeric = False
        for i, row in df.iterrows():
            if i == 5:
                break
            shipdate = row.shipdate
            if isinstance(shipdate, int):
                shipdate_is_numeric = True
            elif isinstance(shipdate, float):
                df = df.astype({'shipdate': 'float'})
                df = df.astype({'shipdate': 'int'})
                shipdate_is_numeric = True
            break
        if shipdate_is_numeric:
            df['shipdate'] = df.apply(lambda row: datetime.fromordinal(
                datetime(1900, 1, 1).toordinal() + int(row['shipdate']) - 2), axis=1)
        else:
            df['shipdate'] = pd.to_datetime(df['shipdate'])
    return df