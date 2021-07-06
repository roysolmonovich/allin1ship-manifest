import pandas as pd
import numpy as np
from datetime import datetime

from models.manifest import ManifestRaw, ManifestModel


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


def generate_defaults(df, zone_weights, start_date, end_date, def_domestic, def_international):
    generated_columns = {}
    if 'country' not in df.columns:
        df['country'] = 'US'
        generated_columns['country'] = 'country_gen'
    if 'zip' not in df.columns:
        df['zip'] = 'N/A'
        zones_df = pd.DataFrame([tuple(zone_weight.keys())[0] for zone_weight in zone_weights])
        weights = [tuple(zone_weight.values())[0] for zone_weight in zone_weights]
        df['zone'] = zones_df.sample(len(df.index), weights=weights, replace=True).reset_index()[0]
        generated_columns['zip'] = 'zip_gen'
    if 'shipdate' not in df.columns:
        # global start_date, end_date
        # start_date, end_date = pd.to_datetime(start_date), pd.to_datetime(end_date)
        df['shipdate'] = ManifestModel.random_dates(pd.to_datetime(
            start_date), pd.to_datetime(end_date), len(df.index)).sort_values()
        # df['shipdate'] = df.apply(lambda row: mflib.rand_date_str(row), axis=1)
        # df['shipdate'] = df['shipdate'].dt.strftime('%Y-%m-%d')
        generated_columns['shipdate'] = 'shipdate_gen'
    if 'service' not in df.columns:
        df['service'] = np.where(df['country'] == 'US', def_domestic, def_international)
        generated_columns['service'] = 'service_gen'
    return df, generated_columns