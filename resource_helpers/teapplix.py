

from resource_helpers.manifest import create_df
from models.manifest import ManifestModel


def teapplix(
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
    pot_columns = {
        'orderno': orderno, 
        'shipdate': shipdate, 
        'weight': weight,
        'service': sv, 
        'zip': address, 
        'country': address_country, 
        'price': current_price, 
        'insured': insured_parcel
    }
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
    sv_main, sv_alt0, sv_alt1 = False, False, False
    if 'service' in headers.values():
        sv_main = True
    else:
        if sv['header_alt'][0][0] in col_set:
            sv_alt0 = True
            columns.append(sv['header_alt'][0][0])
            headers[columns[-1]] = 'service_provider'
            dtype[headers[columns[-1]]] = ManifestModel.type_conv[sv['format']]
        elif sv['header_alt'][0][1] in col_set:
            sv_alt0 = True
            columns.append(sv['header_alt'][0][1])
            headers[columns[-1]] = 'service_provider'
            dtype[headers[columns[-1]]] = ManifestModel.type_conv[sv['format']]
        if sv['header_alt'][1][0] in col_set:
            sv_alt1 = True
            columns.append(sv['header_alt'][1][0])
            headers[columns[-1]] = 'service_code'
            dtype[headers[columns[-1]]] = ManifestModel.type_conv[sv['format']]
        elif sv['header_alt'][1][1] in col_set:
            sv_alt1 = True
            columns.append(sv['header_alt'][1][1])
            headers[columns[-1]] = 'service_code'
            dtype[headers[columns[-1]]] = ManifestModel.type_conv[sv['format']]
    if weight_name:
        df.dropna(subset=[weight_name], inplace=True)
        for i, row in df.iterrows():
            weight_test = str(row[weight_name])
            if weight_test.replace('.', '').isnumeric():
                dtype[weight_name] = weight['format']
                df = create_df(columns, dtype, headers, pf, filename, api_file_path, name)
                break
            elif 'oz' in weight_test or 'lb' in weight_test or 'lbs' in weight_test:
                dtype[weight_name] = 'str'
                df = create_df(columns, dtype, headers, pf, filename, api_file_path, name)
                df['weight'] = df.apply(lambda row: ManifestModel.w_lbs_or_w_oz(row['weight']), axis=1)
                break
    else:
        df = create_df(columns, dtype, headers, pf, filename, api_file_path, name)
    if sv_alt0 and sv_alt1:
        df['service'] = df['service_provider'].combine(df['service_code'], lambda x1, x2: f'{x1} {x2}')
        del df['service_provider']
        del df['service_code']
    # At this point we either have a service column with vendor and service code,
    # or a service code column with an optional service provider column.
    # If service provider is given, concatenate with service code.
    empty_cols = ManifestModel.ai1s_headers.difference(
        set(df.columns)).difference({'shipdate', 'zip', 'country', 'service provider and name', 'service provider', 'service name', 'address'})
    for col in empty_cols:
        df[col] = None
    return df, empty_cols