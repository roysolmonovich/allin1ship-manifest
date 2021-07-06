from models.manifest import ManifestModel
from resource_helpers.manifest import create_df

def sellercloud_shipbridge(
    col_set, 
    df,
    orderno,
    shipdate,
    weight,
    sv,
    address,
    address_country,
    current_price,
    insured_parcel,
    pf, filename, api_file_path, name
):
    columns = []
    dtype = {}
    headers = {}
    not_found = []
    weight_name = None
    pot_columns = {'orderno': orderno, 'shipdate': shipdate, 'weight': weight,
                    'service': sv, 'address': address, 'price': current_price}
    for pot_c in pot_columns.keys():
        for col in pot_columns[pot_c]['header']:
            found = False
            if col in col_set:
                if pot_c != 'weight':
                    dtype[col] = pot_columns[pot_c]['format']
                else:
                    weight_name = col
                headers[col] = pot_c
                found = True
                break
        if not found:
            not_found.append(pot_c)
    columns = list(headers.keys())
    if weight_name:
        weight_test = str(df[weight_name].iloc[0])
        if weight_test.replace('.', '').isnumeric():
            dtype[weight_name] = weight['format']
            df = create_df(columns, dtype, headers, pf, filename, api_file_path, name)
        elif 'oz' in weight_test or 'lb' in weight_test or 'lbs' in weight_test:
            dtype[weight_name] = 'str'
            df = create_df(columns, dtype, headers, pf, filename, api_file_path, name)
            df['weight'] = df.apply(lambda row: ManifestModel.w_lbs_or_w_oz(row['weight']), axis=1)
    else:
        df = create_df(columns, dtype, headers, pf, filename, api_file_path, name)
    df['weight'] *= 16
    df[['zip', 'country']] = df.apply(lambda row: ManifestModel.add_to_zip_ctry(
        row.address), axis=1, result_type='expand')
    # At this point we either have a service column with vendor and service code,
    # or a service code column with an optional service provider column.
    # If service provider is given, concatenate with service code.
    empty_cols = ManifestModel.ai1s_headers.difference(
        set(df.columns)).difference({'shipdate', 'zip', 'country', 'service provider and name', 'service provider', 'service name', 'address'})
    for col in empty_cols:
        df[col] = None
    return df, empty_cols