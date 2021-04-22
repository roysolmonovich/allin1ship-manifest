import csv
import lib
import os
import re
from pathlib import Path
import sys
import json
import schedule
import time
import numpy as np
import pandas as pd
from datetime import datetime, date, timedelta
from gmail import process_emails, shipping_variance_log, marked_up_log, overlabeled_log
from ftplib import FTP

overlabeled = lib.overlabeled
amp = lib.amp
usps_zip_zone_2019 = lib.usps_zip_zone_2019
usps_zip_zone_2020 = lib.usps_zip_zone_2020
dhl_zip_zone_2019 = lib.dhl_zip_zone_2019
dhl_zip_zone_2020 = lib.dhl_zip_zone_2020
ca_zip_zone = lib.ca_zip_zone
intl_names = lib.intl_names
service = lib.service
flag_map = lib.flag_map
cust_names = lib.cust_names
del_to_dim = lib.del_to_dim
invoices = lib.invoices
mydb = lib.Customer.mydb
mycursor = mydb.cursor()
# mydb = mysql.connector.connect(
#     host="162.241.219.134",
#     user="allinoy4_user0",
#     password="+3mp0r@ry",
#     database="allinoy4_allin1ship"
# )
# print(mydb)
# mycursor = mydb.cursor()
# None, zone, zone_desc, None,
# lib.CarrierCharge.charge_weight("DHL", "domestic" if domestic else "international",
#                                 ship_date, sc, zone, w[0]), ship_date, None, None] + row[2: 8] + row[9: 79] + [inv_no, bill_date if acc[0] else None,
sql_mh = "INSERT INTO dhl_master_invoice \
(account, usps_comp_rate, charge, surcharges, proper_charge, total_charges_dhl, proper_zone, zone_formatted, \
weight_oz, ship_date, inventory_positioner, bol_number, billing_ref, billing_ref2, processing_facility, internal_tracking, \
customer_confirm, delivery_confirm, recipient_name, recipient_address1, recipient_address2, recipient_city, recipient_state, recipient_zip, recipient_country, material_or_vas_num, \
actual_weight, billing_weight, quantity, uom_quantity, pricing_zone, charge_dhl, cust_reference1, \
cust_reference2, workshare_dropoff, workshare_sort, workshare_stamp, workshare_machine, workshare_manifest, \
workshare_bpm, workshare_future_use, workshare_future_use2, workshare_future_use3, \
surcharge_content_endorse, surcharge_unassignable_add, surcharge_special_handling, surcharge_late_arrival, \
surcharge_usps_qualif, surcharge_client_srd, surcharge_irregular, returnedmail_unassignable, \
returnedmail_unprocessable, returnedmail_recall, returnedmail_duplicate, returnedmail_cont_assur, \
returnedmail_move_update, gst_tax, hst_tax, pst_tax, vat_tax, duties, tax, returnedmail_paper_invoice, \
returnedmail_screening, returnedmail_non_auto_flats, returnedmail_future_use, surcharge_fuel, \
min_pickup_charge, overlabeled_value, dim_weight, dim_length, dim_width, \
dim_height, uom_dims, reserved_future_use, reserved_future_use4, reserved_future_use5, reserved_future_use6, \
reserved_future_use7, reserved_future_use8, invoice, bill_date, \
ai1s_service) \
VALUES \
(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, \
%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, \
%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, \
%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, \
%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, \
%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, \
%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, \
%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, \
%s, %s, %s)"
sql_update = 'UPDATE dhl_master_invoice \
SET account = %s, charge = %s, bill_date = %s\
WHERE ship_date = %s \
AND delivery_confirm = %s \
AND material_or_vas_num = %s'
sql_dispute = "INSERT INTO to_dispute (ship_date, delivery_confirm, material_or_vas_num, \
ai1s_charge, carrier_charge, charge_desc, reason, status) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
sql_invoices_init = "INSERT IGNORE INTO invoices (inv_no, vendor, inv_date, amount_stated) VALUES (%s, 'DHL', %s, %s)"


def inv_file_sep(invoice_processing_time, inv_date=None, sql=None, history_corrected=None, ship_keys=None):
    print(invoice_processing_time, inv_date, sql, history_corrected, ship_keys)
    if not sql:
        sql = 'SELECT `account`, `Ship Date`, `Customer Reference`, `Customer Confirmation`, \
            `Delivery Confirmation`, `Recipient Address`, `Service`, `Weight (oz)`, `Pricing Zone`, \
            `USPS Comp. Rate`, `Charge`, `Surcharges`, `Total Savings`, `Total Charges` \
            FROM invoice_view WHERE bill_date = %s'
        mycursor.execute(sql, (inv_date, ))
    else:
        mycursor.executemany(sql, ship_keys)
    customer_bills = {}
    while True:
        row = mycursor.fetchone()
        if not row:
            break
        row = list(row)
        row[6] = service[str(row[6])][3]
        if not customer_bills.get(row[0]):
            customer_bills[row[0]] = []
        customer_bills[row[0]].append(row[1:])
    if not history_corrected:
        Path(rf"{os.getcwd()}\{invoice_processing_time} invoices").mkdir(parents=True, exist_ok=True)
    else:
        Path(rf"{os.getcwd()}\{invoice_processing_time} invoices - history corrected records").mkdir(parents=True, exist_ok=True)
    for c in customer_bills:
        file = rf'{invoice_processing_time} invoices\{lib.Customer.crm[str(c)]["name"] if c > 0 else "acc_not_found"} - {inv_date}.csv' \
            if not history_corrected else \
            rf'{invoice_processing_time} invoices - history corrected records\{lib.Customer.crm[str(c)]["name"] if c > 0 else "acc_not_found"} - {inv_date}.csv'
        with open(file, 'w', encoding="utf-8") as f1:
            write = csv.writer(f1, delimiter=',', lineterminator='\n')
            write.writerow(('Ship Date', 'Customer Reference', 'Customer Confirmation',
                            'Delivery Confirmation', 'Recipient Address', 'Service',
                            'Weight (oz)', 'Pricing Zone', 'USPS Comp. Rate', 'Charge',
                            'Surcharges', 'Total Savings', 'Total Charges'))
        with open(file, 'a', encoding="utf-8") as f1:
            write = csv.writer(f1, delimiter=',', lineterminator='\n')
            write.writerows(customer_bills[c])
    # for c in cust_bills:
    #     file=rf'temp\{cust_names[c] if isinstance(c, int) else c} - {bill_date}.csv'
    #     if not os.path.exists(file):
    #         with open(file, 'w', encoding = "utf-8") as f1:
    #             write=csv.writer(f1, delimiter = ',', lineterminator = '\n')
    #             write.writerow(('Ship Date', 'Customer Reference', 'Customer Confirmation',
    #                             'Delivery Confirmation', 'Recipient Address', 'Service',
    #                             'Weight (oz)', 'Pricing Zone', 'USPS Comp. Rate', 'Charge',
    #                             'Surcharges', 'Total Savings', 'Total Charges'))
    #     with open(file, 'a', encoding = "utf-8") as f1:
    #         write=csv.writer(f1, delimiter = ',', lineterminator = '\n')
    #         write.writerows(cust_bills[c])


def inv_file_master(invoice_processing_time, inv_date=None, sql=None, history_corrected=None, ship_keys=None):
    res = None
    if not sql:
        sql = 'SELECT `account`, `Ship Date`, `Customer Reference`, `Customer Confirmation`, \
            `Delivery Confirmation`, `Recipient Address`, `Service`, `Weight (oz)`, `Pricing Zone`, \
            `USPS Comp. Rate`, `Charge`, `Surcharges`, `Total Savings`, `Total Charges` \
            FROM invoice_view WHERE bill_date = %s'
        mycursor.execute(sql, (inv_date, ))
        res = mycursor.fetchall()
    else:
        if ship_keys:
            mycursor.executemany(sql, ship_keys)
            res = [mycursor.fetchone()]
            if res[0]:
                res += mycursor.fetchall()
            else:
                return
    if not history_corrected:
        Path(rf"{os.getcwd()}\{invoice_processing_time} invoices").mkdir(parents=True, exist_ok=True)
        file = rf'{invoice_processing_time} invoices\master invoice.csv'
    else:
        Path(rf"{os.getcwd()}\{invoice_processing_time} invoices - history corrected records").mkdir(parents=True, exist_ok=True)
        file = rf'{invoice_processing_time} invoices - history corrected records\master invoice.csv'
    with open(file, 'w', encoding="utf-8") as f1:
        write = csv.writer(f1, delimiter=',', lineterminator='\n')
        write.writerow(('Account #', 'Ship Date', 'Customer Reference', 'Customer Confirmation',
                        'Delivery Confirmation', 'Recipient Address', 'Service',
                        'Weight (oz)', 'Pricing Zone', 'USPS Comp. Rate', 'Charge',
                        'Surcharges', 'Total Savings', 'Total Charges'))
    with open(file, 'a', encoding="utf-8") as f1:
        write = csv.writer(f1, delimiter=',', lineterminator='\n')
        write.writerows(res)

    # for c in cust_bills:
    #     file = rf'temp\master invoice - {inv_no} - {bill_date}.csv'
    #     if not os.path.exists(file):
    #         with open(file, 'w', encoding="utf-8") as f1:
    #             write = csv.writer(f1, delimiter=',', lineterminator='\n')
    #             write.writerow(('Account #', 'Ship Date', 'Customer Reference', 'Customer Confirmation',
    #                             'Delivery Confirmation', 'Recipient Address', 'Service',
    #                             'Weight (oz)', 'Pricing Zone', 'USPS Comp. Rate', 'Charge',
    #                             'Surcharges', 'Total Savings', 'Total Charges'))
    #     with open(file, 'a', encoding="utf-8") as f1:
    #         write = csv.writer(f1, delimiter=',', lineterminator='\n')
    #         write.writerows(cust_bills[c])


# def account_sum_file(each_cust_sum, inv_no, bill_date):
def account_sum_file(invoice_processing_time, inv_date=None, sql=None, history_corrected=None, ship_keys=None):
    if not sql:
        sql = 'SELECT account, SUM(charge), SUM(surcharges), SUM(charge+surcharges), SUM(usps_comp_rate), \
        SUM(usps_comp_rate-charge), SUM(total_charges_dhl) \
        FROM dhl_master_invoice JOIN invoices ON invoice = inv_no \
        WHERE bill_date = %s AND account > 0 \
        GROUP BY account \
        '
        mycursor.execute(sql, (inv_date, ))
    else:
        mycursor.executemany(sql, ship_keys)
    res = []
    while True:
        row = mycursor.fetchone()
        if row:
            res.append(row)
        else:
            break
    if not res:
        return
    headers = ['Account #', 'Charge', 'Surcharge', 'Total Charge', 'USPS', 'Savings', 'Total DHL Invoice']
    df = pd.DataFrame(res, columns=headers, dtype=float)
    df['Account #'] = df['Account #'].astype(int)
    df['Account Name'] = df.apply(lambda row: lib.Customer.crm[str(int(row['Account #']))]
                                  ['name'] if row['Account #'] > 0 else np.nan, axis=1)
    df['Payment Type'] = df.apply(lambda row: lib.Customer.crm[str(int(row['Account #']))].get(
        'payment method') if row['Account #'] > 0 else np.nan, axis=1)
    df['Fee Rate'] = df.apply(lambda row: lib.Customer.crm[str(int(row['Account #']))].get('fee')
                              if row['Account #'] > 0 else np.nan, axis=1)
    df['Total Charge'] = df['Total Charge'].astype(float)
    pd.set_option("precision", 4)
    df['Fee $ Amount'] = (df['Fee Rate']*df['Total Charge']).astype(float)
    # df.style.format({'Fee $ Amount': '{:.2f}'})
    df['Fee $ Amount'] = df['Fee $ Amount'].map(lambda x: '%0.2f' % x).astype(float)
    df['Profit'] = df['Charge']+df['Surcharge']-df['Total DHL Invoice']
    file_headers = ['Account Name', 'Account #', 'Payment Type', 'Fee Rate', 'Total Charge', 'Fee $ Amount',
                    'Savings', 'Surcharge', 'Charge', 'USPS', 'Total DHL Invoice', 'Profit']
    df = df[file_headers]
    if not history_corrected:
        Path(rf"{os.getcwd()}\{invoice_processing_time} invoices").mkdir(parents=True, exist_ok=True)
        file = rf'{invoice_processing_time} invoices\customer sum charges.xlsx'
    else:
        Path(rf"{os.getcwd()}\{invoice_processing_time} invoices - history corrected records").mkdir(parents=True, exist_ok=True)
        file = rf'{invoice_processing_time} invoices - history corrected records\customer sum charges.xlsx'
    df.sort_values(by=['Payment Type', 'Total Charge'], ascending=[True, False], inplace=True)
    # df.insert(loc=0, column='Status', value=[np.nan for i in range(len(df.index))])

    writer = pd.ExcelWriter(file, engine='openpyxl')
    status = pd.DataFrame([np.nan], columns=['Status'])
    status.to_excel(writer, index=False)
    # df.to_excel(file, columns=file_headers, index=False)
    df.to_excel(writer, columns=file_headers, startcol=1, index=False)
    # Between Sunday and Saturday, evaluates to past Saturday
    if not inv_date:
        d = date.today()
        inv_date = d-timedelta(days=d.toordinal() % 7) if not inv_date else inv_date
    past_sat = inv_date-timedelta(days=1)
    past_sat_str = "%01d/%01d/%d" % (past_sat.month, past_sat.day, past_sat.year)
    prev_sun = past_sat-timedelta(days=6)  # Between Sunday and Saturday, evaluates to previous Sunday
    prev_sun_str = "%01d/%01d/%d" % (prev_sun.month, prev_sun.day, prev_sun.year)
    billing_period = pd.DataFrame([f'Billing Period {prev_sun_str} - {past_sat_str}'], columns=['Billing Period'])
    billing_period.to_excel(writer, startcol=len(df.columns)+2, index=False)
    writer.save()
    writer.close()


def correct_history_recs(invoice_processing_time):
    d = date.today()
    bill_date = d-timedelta(days=d.toordinal() % 7)  # Current week's Sunday
    sql = "SELECT bol_number, pricing_zone, overlabeled_value, customer_confirm, \
    internal_tracking, recipient_zip, dhl_master_invoice.delivery_confirm, dhl_master_invoice.ship_date, material_or_vas_num, weight_oz \
    FROM dhl_master_invoice LEFT JOIN duplicates ON dhl_master_invoice.delivery_confirm = duplicates.delivery_confirm \
    AND dhl_master_invoice.ship_date = duplicates.ship_date AND material_or_vas_num = service \
    WHERE duplicates.delivery_confirm IS NULL AND (bill_date IS NULL) \
    AND dhl_master_invoice.delivery_confirm != 'JJD0002251914547380'"
    mycursor.execute(sql)
    res = mycursor.fetchall()
    dic1 = {}
    dic2 = {}
    mh_vals_bulk = []
    for r in res:
        bol, zone, ol, cust_conf, int_tr_no, zip, deliv_confirm = r[:7]
        zip = zip if zip else ''
        domestic = True if zone[:4] == 'USPS' else False
        # key = (r[6], str(r[7]), r[8])
        key = (r[6], str(r[7]), r[8])
        sql_key = (r[6] if not ol else str(ol), str(r[7]), r[8])
        bol = str(bol)
        deliv_confirm = str(deliv_confirm)
        ol = str(ol)
        cust_conf = str(cust_conf)
        sc = key[2]
        if sc == 36:
            sc = 81
        elif sc == 83:
            sc = 82
        if not domestic:
            if zone[:2] == 'CA' and str(sc) == 60:
                zone = lib.ca_zip_zone[zip[:3]]
            else:
                zone = zone[:2]
        int_tr_no = str(int_tr_no)
        w = (r[-1], True)
        if zip.find('-') != -1:
            zip = zip[:zip.find('-')]
        acc = lib.acc_num(bol, deliv_confirm, zone, ol, cust_conf, int_tr_no, zip)
        if acc[0]:
            dic1[key] = acc
            dic2[sql_key] = acc
            rate = lib.Customer.recent_tier(
                str(acc[0]), 'domestic' if domestic else 'international', key[1])
            if rate == 'COST':
                our_c = None
            else:
                our_c = lib.CarrierCharge.charge_rate(
                    rate, 'domestic' if domestic else 'international', key[1], sc, zone, w[0])
            mh_vals_bulk.append((acc[0], our_c, key[1], deliv_confirm, key[2]))

    if not dic1:
        print('No new accounts to correct found...')
        return

    sql_update1 = 'UPDATE dhl_master_invoice \
    SET account = %s, charge = %s \
    WHERE ship_date = %s \
    AND delivery_confirm = %s \
    AND material_or_vas_num = %s \
    AND account < 0;'
    print('Following shipments are being corrected:')
    print(dic1.keys())
    mycursor.executemany(sql_update1, mh_vals_bulk)
    # invoice_processing_time = datetime.now().strftime('%y%m%d %H %M %S')
    sql_inv = 'SELECT `account`, `Ship Date`, `Customer Reference`, `Customer Confirmation`, \
            `Delivery Confirmation`, `Recipient Address`, `Service`, `Weight (oz)`, `Pricing Zone`, \
            `USPS Comp. Rate`, `Charge`, `Surcharges`, `Total Savings`, `Total Charges` \
            FROM invoice_view WHERE (RIGHT(`Delivery Confirmation`, LENGTH(`Delivery Confirmation`)-1), `Ship Date`, `Service`) IN ((%s, %s, %s))'
    # mycursor.executemany(sql_inv, list(dic2.keys()))
    # print(mycursor.fetchall())
    sql_acc_sum = 'SELECT account, SUM(charge), SUM(surcharges), SUM(charge+surcharges), SUM(usps_comp_rate), \
            SUM(usps_comp_rate-charge), SUM(total_charges_dhl) \
            FROM dhl_master_invoice JOIN invoices ON invoice = inv_no \
            WHERE (dhl_master_invoice.delivery_confirm, dhl_master_invoice.ship_date, dhl_master_invoice.material_or_vas_num) IN \
            ((%s, %s, %s)) GROUP BY account'
    # + str(tuple(dic1.keys())) + GROUP BY account
    # mycursor.execute(sql_acc_sum)
    # print(mycursor.fetchall())
    inv_file_sep(invoice_processing_time, sql=sql_inv, history_corrected=True, ship_keys=list(dic2.keys()))
    inv_file_master(invoice_processing_time, sql=sql_inv, history_corrected=True, ship_keys=list(dic2.keys()))
    account_sum_file(invoice_processing_time, sql=sql_acc_sum, history_corrected=True, ship_keys=list(dic1.keys()))
    corrected_count_sql = 'SELECT COUNT(*) FROM dhl_master_invoice WHERE bill_date IS NULL and (delivery_confirm, ship_date, material_or_vas_num) IN ((%s, %s, %s))'
    mycursor.executemany(corrected_count_sql, list(dic1.keys()))
    count_before = mycursor.fetchone()
    sql_update2 = f"UPDATE dhl_master_invoice \
        SET bill_date = '{bill_date}' WHERE (delivery_confirm, ship_date, material_or_vas_num) IN ((%s, %s, %s))"
    mycursor.executemany(sql_update2, list(dic1.keys()))
    mydb.commit()
    mycursor.executemany(corrected_count_sql, list(dic1.keys()))
    count_after = mycursor.fetchone()
    print(f'{count_before}-{count_after} records corrected.')


def dispute_file(invoice_processing_time, inv_date):
    sql = 'SELECT dhl_master_invoice.invoice, to_dispute.ship_date, CONCAT("\'", to_dispute.delivery_confirm), \
    to_dispute.material_or_vas_num, ai1s_charge, carrier_charge, charge_desc, reason, \
    carrier_charge-ai1s_charge \
    FROM to_dispute JOIN dhl_master_invoice \
    ON dhl_master_invoice.delivery_confirm = to_dispute.delivery_confirm \
    AND dhl_master_invoice.material_or_vas_num = to_dispute.material_or_vas_num \
    AND dhl_master_invoice.ship_date = to_dispute.ship_date \
    JOIN invoices ON dhl_master_invoice.invoice = inv_no \
    WHERE inv_date = %s'
    mycursor.execute(sql, (inv_date, ))
    res = mycursor.fetchall()
    Path(rf"{os.getcwd()}\{invoice_processing_time} invoices").mkdir(parents=True, exist_ok=True)
    file = rf'{invoice_processing_time} invoices\DHL disputes.csv'
    if not os.path.exists(file):
        with open(file, 'w', encoding="utf-8") as f1:
            write = csv.writer(f1, delimiter=',', lineterminator='\n')
            write.writerow(('invoice', 'ship_date', 'delivery_confirm', 'material_or_vas_num',
                            'ai1s_charge', 'carrier_charge', 'charge_desc', 'reason', 'disputed_amount', 'credit_memo', 'total_issued'))
    with open(file, 'a', encoding="utf-8") as f1:
        write = csv.writer(f1, delimiter=',', lineterminator='\n')
        write.writerows(res)


def missing_accounts_file(invoice_processing_time):
    sql = 'SELECT dhl_master_invoice.* FROM dhl_master_invoice LEFT JOIN duplicates \
        ON dhl_master_invoice.delivery_confirm = duplicates.delivery_confirm \
        AND dhl_master_invoice.ship_date = duplicates.ship_date \
        AND material_or_vas_num = service \
        WHERE duplicates.delivery_confirm IS NULL \
        AND account < 0;'
    mycursor.execute(sql)
    res = mycursor.fetchall()
    Path(rf"{os.getcwd()}\{invoice_processing_time} invoices").mkdir(parents=True, exist_ok=True)
    file = rf'{invoice_processing_time} invoices\DHL history accounts not found (non duplicates).csv'
    if not os.path.exists(file):
        pass
        # with open(file, 'w', encoding="utf-8") as f1:
        #     write = csv.writer(f1, delimiter=',', lineterminator='\n')
        #     write.writerow(('invoice', 'ship_date', 'delivery_confirm', 'material_or_vas_num',
        #                     'ai1s_charge', 'carrier_charge', 'charge_desc', 'reason', 'disputed_amount', 'credit_memo', 'total_issued'))
    with open(file, 'a', encoding="utf-8") as f1:
        write = csv.writer(f1, delimiter=',', lineterminator='\n')
        write.writerows(res)


def generate_bills(**kwargs):
    files = []
    separate_inv_file = kwargs.get('separate_inv_file')
    master_inv_file = kwargs.get('master_inv_file')
    sum_file = kwargs.get('sum_file')
    disputes_file = kwargs.get('disputes_file')
    to_sql_mh = kwargs.get('to_sql_mh')
    create_profit_report = kwargs.get('create_profit_report')
    to_sql_dispute = kwargs.get('to_sql_dispute')
    manual_acc_input = kwargs.get('manual_acc_input')
    check_duplicates = kwargs.get('check_duplicates')
    correct_history_accounts = kwargs.get('correct_history_accounts')
    for f in kwargs:
        if isinstance(f, str) and f[: 4] == 'file':
            files.append(kwargs[f])
    d = date.today()
    bill_date = d-timedelta(days=d.toordinal() % 7)  # Current week's Sunday
    # each_cust_sum = {}
    # cust_bills_separate = {}
    # cust_bills_master = {}
    dispute = []
    sql_dispute_vals_bulk = []
    # time of run - invoice folder is created based on that time
    invoice_processing_time = datetime.now().strftime('%y%m%d %H %M %S')
    Path(rf"{os.getcwd()}\{invoice_processing_time} invoices").mkdir(parents=True, exist_ok=True)
    if len(files) <= 2:
        accs_not_found_file = f'temp\{" - ".join(files).replace(".csv", "")} acc not found.csv'
    else:
        accs_not_found_file = f'temp\{files[0].rstrip(".csv")} - {len(files)} invoices acc not found.csv'
    with open(accs_not_found_file, 'w', encoding="utf-8") as f_acc:
        for file in files:
            with open(f'invoices\{file}', encoding="unicode_escape") as csv_file:
                mh_vals_bulk = []
                write = csv.writer(f_acc, delimiter=',', lineterminator='\n')
                csv_reader = csv.reader(csv_file, delimiter=',')
                row = next(csv_reader)
                write.writerow(row + ['Invoice records of not found accounts'])
                total_charges = {'stated': round(float(row[11]), 2), 'added_up': 0}
                inv_no = int(row[2])
                inv_date = datetime.strptime(row[1], '%Y%m%d').date()
                val = (inv_no, inv_date, total_charges['stated'])
                mycursor.execute(sql_invoices_init, val)
                print(f'Starting invoice {inv_no}')
                line_count = 1
                prev_s = False
                count_not_found = 0
                cust_confirm_to_shipment = {}    # Duplicate bills pass one of the following conditions:
                ol_to_shipment = {}             # 1) Delivery Convfirmation will be found within a previous Customer
                del_confirm_to_shipment = {}  # Confirmation's last 22 characters.
                # 2) Delivery Convfirmation will have an exact match with a previous Overlabeled record.
                # 3) A combination of Delivery Confirmation and Service Code (exact matches)
                #    will be found in a previous bill.
                #    Sets of Customer Confirmations (last 22 characters), Overlabeled, and
                #    (Delivery Confirmation, Service Code) tuples are stored to catch duplicates.

                warning_log = {}
                potential_dupl_flagged = []
                potential_dupl_unflagged = []
                for row in csv_reader:
                    line_count += 1
                    if not (line_count-1) % 1000:
                        print(f"Processed {line_count-1} records.")
                    ship_date = datetime.strptime(row[8], '%Y%m%d').date()
                    del_confirm = row[12]
                    cust_confirm = row[11]
                    ol = row[66]
                    bol_number = row[3]
                    inv_sc = int(row[20])
                    zip = row[18]
                    billing_ref, billing_ref2 = row[4:6]
                    if 'return' in billing_ref.lower() or 'return' in billing_ref2.lower():
                        warning_log[(line_count, 'Return in billing refs')] = row
                    domestic = True if row[28][: 4] == "USPS" else False
                    if not prev_s and inv_sc == 154 and row[29] == '0':
                        continue
                    if prev_s and inv_sc == 154:
                        dhl_c = .35
                        usps_c = 0
                        our_c = .35
                        sc_desc = service["154"][3]
                        zone_desc = None
                        sc = 154
                    elif inv_sc == 192:
                        dhl_c = 2.85
                        usps_c = None
                        our_c = 2.85
                        sc_desc = service["192"][3]
                        zone_desc = None
                        sc = 192
                    else:
                        zip = row[18]
                        if domestic:
                            if len(zip) <= 5:
                                zip3 = int(zip[: len(zip)-2])
                            else:
                                zip3 = int(zip[: len(zip)-6])
                                zip = zip[: -4] + '-' + zip[-4:]
                            row[18] = zip
                            if ship_date < date(2020, 3, 1):
                                zone = usps_zip_zone_2019[zip3] if inv_sc in (70, 261) else dhl_zip_zone_2019[zip3]
                            else:
                                zone = usps_zip_zone_2020[zip3] if inv_sc in (70, 261) else dhl_zip_zone_2020[zip3]
                            if zone == '09':
                                zone = '08'
                            zone = "USPS" + zone
                            zone_desc = 'Zone ' + zone[-2:]
                        else:
                            zip = zip.lstrip('0')
                            zone = row[28]
                            zone_desc = intl_names.get(zone, 'N/A')
                            if zone[:2] == 'CA' and inv_sc == 60:
                                zone = ca_zip_zone[zip[:3]]
                            else:
                                zone = zone[:2]

                        acc = (int(row[-1]), 1) if manual_acc_input \
                            else lib.route_acc(lib.acc_num(bol_number, del_confirm, zone, ol, cust_confirm, row[10], zip), ship_date)

                        # if acc[0] and acc[1] in (0, -9) and domestic and inv_sc not in (154, 192):
                        if acc[0] and domestic and inv_sc not in (154, 192):
                            if acc[1] == 0:
                                # ship_date, material_or_vas_num, delivery_confirm, RIGHT(customer_confirm, 22), overlabeled_value, account
                                potential_dupl_unflagged.append((ship_date, inv_sc, del_confirm, acc[0]))
                            elif acc[1] == -9:
                                potential_dupl_flagged.append(
                                    (ship_date, inv_sc, del_confirm, cust_confirm[-22:], acc[0]))
                                # l_master_invoice.ship_date, material_or_vas_num, dhl_master_invoice.delivery_confirm, RIGHT(customer_confirm, 22), \
                                #     account, bol_number, pricing_zone, internal_tracking, recipient_zip
                        # if cust_confirm == 'GM545345286092010622':
                        #     acc = (5344480, acc[1])
                        # if GM545345286092010622 -> 5344480
                        # if acc == -8:
                        #     acc = 5351687
                        # #MANUAL OVERRIDE #####
                        if not acc[0]:
                            our_c = None
                            count_not_found += 1
                            write.writerow(row + [flag_map[acc[1]]])
                        if row[69] == '0':
                            replace_dims = del_to_dim.get(row[12])
                            if replace_dims is not None:
                                (row[69], row[70], row[71]) = replace_dims
                        w = lib.weight(float(row[22]), zone, float(row[69]), float(row[70]), float(row[71])) \
                            if row[69] != '' else lib.weight(float(row[22]), zone)
                        if not domestic:
                            for _ in (22, 24, 67):
                                row[_] = "{:.2f}".format(float(row[_])*16)
                        sc = lib.service[str(inv_sc)][0] if domestic else inv_sc
                        charge_weight = lib.CarrierCharge.charge_weight("DHL", "domestic" if domestic else "international",
                                                                        str(ship_date), sc, zone, w[0])
                        if sc == 81:
                            if charge_weight < 16:
                                sc_desc = service["81"][3]
                            else:
                                sc_desc = service["36"][3]
                        elif sc == 82:
                            if charge_weight < 16:
                                sc_desc = service["82"][3]
                            else:
                                sc_desc = service["83"][3]
                        else:
                            sc_desc = service[str(inv_sc)][3]
                        dhl_c = lib.CarrierCharge.charge_rate("DHL", "domestic" if domestic else "international",
                                                              str(ship_date), sc, zone, w[0])
                        dhl_c = round(dhl_c, 2) if dhl_c is not None else None
                        usps_c = lib.CarrierCharge.charge_rate("USPS", "domestic" if domestic else "international",
                                                               str(ship_date), sc, zone, w[0])
                        usps_c = round(usps_c, 2) if usps_c is not None else None
                        if usps_c == -2:
                            usps_c = None
                        if acc[0]:
                            rate = lib.Customer.recent_tier(
                                str(acc[0]), 'domestic' if domestic else 'international', str(ship_date))
                            if rate == 'COST':
                                our_c = float(row[29])
                            else:
                                our_c = lib.CarrierCharge.charge_rate(
                                    rate, 'domestic' if domestic else 'international', str(ship_date), sc, zone, w[0])
                                if our_c is None or our_c <= 0:
                                    our_c = None
                                    warning_log[(line_count, 'Charge not found')] = row
                        else:
                            rate = None
                            our_c = None
                            warning_log[(line_count, 'Account not found')] = [acc[1]]+row
                        if domestic:
                            if len(cust_confirm) >= 22:
                                if cust_confirm[-22:] not in cust_confirm_to_shipment:
                                    cust_confirm_to_shipment[cust_confirm[-22:]] = []
                                cust_confirm_to_shipment[cust_confirm[-22:]
                                                         ].append((del_confirm, str(ship_date), inv_sc))
                            if ol and ol.isnumeric():
                                if int(ol) not in ol_to_shipment:
                                    ol_to_shipment[int(ol)] = []
                                ol_to_shipment[int(ol)].append((del_confirm, str(ship_date), inv_sc))
                            if del_confirm not in del_confirm_to_shipment:
                                del_confirm_to_shipment[del_confirm] = []
                            del_confirm_to_shipment[del_confirm].append((del_confirm, str(ship_date), inv_sc))

                    our_c = round(our_c, 2) if our_c is not None else None
                    dhl_inv_c = round(float(row[29]), 2)
                    dhl_inv_perlb = round(float(row[47]), 2)
                    dhl_inv_temp = round(float(row[73]), 2)
                    dhl_inv_other_surc = round(sum(float(_ if _ else 0) for _ in row[32:47]+row[48:66]+row[74:79]), 2)
                    if sc in (70, 261) and dhl_c != dhl_inv_c:
                        warning_log[(line_count, 'USPS rate card mismatch')] = row
                    total_charges['added_up'] += dhl_inv_c + dhl_inv_perlb + dhl_inv_temp + dhl_inv_other_surc
                    (row[69], row[70], row[71]) = (row[69], row[70], row[71]) if '' not in (row[69:72]) else (0, 0, 0)
                    surcharges = lib.surcharge(sc, domestic, ship_date, w[0], float(
                        row[69]), float(row[70]), float(row[71]))
                    our_surc = surcharges[0]
                    our_surc_t = (our_surc['per_lb'] + our_surc['temp'] + our_surc['70_261']
                                  ) if rate != 'COST' else dhl_inv_perlb+dhl_inv_temp+dhl_inv_other_surc
                    dhl_surc = surcharges[1]
                    if acc[0] and (our_c is None or our_c <= 0):
                        print(our_c, row)
                    # inv_sc += 1000  # For testing purposes in SQL - adding 1000 to service number
                    # row[20] = int(row[20]) + 1000
                    if not manual_acc_input:
                        if acc[1] == 0:
                            if dhl_inv_c > dhl_c:
                                reason = []
                                dispute.append((ship_date, '\''+del_confirm, inv_sc, dhl_c,
                                                dhl_inv_c, 'proper charge'))
                                if to_sql_dispute:
                                    if zone != row[28] and lib.CarrierCharge.charge_rate(
                                            rate, 'domestic' if domestic else 'international', str(ship_date), sc, row[28], w[0]) < our_c:
                                        reason.append(f'DHL zone: {row[28]}, our zone: {zone}')
                                    if float(row[24])*(1 if domestic else 16) > charge_weight:
                                        reason.append(
                                            f'DHL billing weight: {row[24]}, Calc. billing weight: {charge_weight}')
                                    sql_dispute_vals_bulk.append((ship_date, del_confirm, inv_sc, dhl_c,
                                                                  dhl_inv_c, 'proper charge', (', ').join(reason), 0))

                            if dhl_inv_perlb > dhl_surc['per_lb']:
                                dispute.append((ship_date, '\''+del_confirm, inv_sc,
                                                dhl_surc['per_lb'], dhl_inv_perlb, 'per lb surcharge'))
                                if to_sql_dispute:
                                    sql_dispute_vals_bulk.append((ship_date, del_confirm, inv_sc, dhl_surc['per_lb'], dhl_inv_perlb,
                                                                  'per lb surcharge', None, 0))

                            if dhl_inv_temp > dhl_surc['temp']:
                                dispute.append((ship_date, '\''+del_confirm, inv_sc,
                                                dhl_surc['temp'], dhl_inv_temp, 'temp surcharge'))
                                if to_sql_dispute:
                                    sql_dispute_vals_bulk.append((ship_date, del_confirm, inv_sc, dhl_surc['temp'], dhl_inv_temp,
                                                                  'temp surcharge', None, 0))

                        else:
                            if domestic:
                                dispute.append((ship_date, '\''+del_confirm, inv_sc, 0, dhl_inv_c+dhl_inv_perlb +
                                                dhl_inv_temp+dhl_inv_other_surc, 'account not found'))
                                if to_sql_dispute:
                                    sql_dispute_vals_bulk.append((ship_date, del_confirm, inv_sc, 0, dhl_inv_temp+dhl_inv_c+dhl_inv_perlb +
                                                                  dhl_inv_temp+dhl_inv_other_surc, 'account not found', None, 0))

                    prev_s = True if inv_sc in (383, 384) else False
                    if not acc[0]:
                        total_c = None
                    elif our_c is None:
                        total_c = None
                    elif usps_c is None:
                        total_c = round(our_c+our_surc_t, 2)
                    else:
                        total_c = round(our_c+our_surc_t, 2)
                    for i in range(len(row)):
                        if row[i] == '':
                            row[i] = None
                    if to_sql_mh:
                        if not manual_acc_input:
                            mh_vals_bulk.append([acc[0] if acc[0] else acc[1], usps_c, our_c,
                                                 our_surc['per_lb'] + our_surc['temp'] + our_surc['70_261'] if rate != 'COST'
                                                 else dhl_inv_perlb+dhl_inv_temp+dhl_inv_other_surc, dhl_c,
                                                 dhl_inv_c+dhl_inv_perlb+dhl_inv_temp+dhl_inv_other_surc,
                                                 zone, zone_desc, charge_weight, ship_date]
                                                + row[2: 7] + row[10: 21] + [row[22], row[24]]+row[26:67]+[row[67]]+row[69:79]+[inv_no, bill_date if acc[0] else None, sc])
                            for i in range(len(mh_vals_bulk[-1])):
                                if mh_vals_bulk[-1][i] and isinstance(mh_vals_bulk[-1][i], str):
                                    mh_vals_bulk[-1][i] = mh_vals_bulk[-1][i].encode("ascii", "ignore").decode()

                        else:
                            mh_vals_bulk.append((acc[0], our_c, bill_date, ship_date, del_confirm, inv_sc))

            total_charges['added_up'] = round(total_charges['added_up'], 2)
            if total_charges['stated'] != total_charges['added_up']:
                print(
                    f'Total charges according to invoice - {total_charges["stated"]} doesn\'t match added up costs - {total_charges["added_up"]}')
            else:
                print(f'Total charges match invoice number - {total_charges["added_up"]}')
            mycursor.executemany(sql_mh if not manual_acc_input else sql_update, mh_vals_bulk)
            mycursor.executemany(sql_dispute, sql_dispute_vals_bulk)
            mydb.commit()
            # potential_dupl.append([cust_confirm[-22:], del_confirm,
            #                        str(ship_date), inv_sc, line_count, acc[1], row])
            if check_duplicates:
                if potential_dupl_flagged:
                    flagged_duplicate_log = lib.history_duplicates(potential_dupl_flagged)
                    if flagged_duplicate_log:
                        warning_log.update(flagged_duplicate_log)
                if potential_dupl_unflagged:
                    unflagged_duplicate_log = lib.invoice_duplicates(inv_no, potential_dupl_unflagged)
                    if unflagged_duplicate_log:
                        warning_log.update(unflagged_duplicate_log)
                missing_accounts_file(invoice_processing_time)
                # duplicate updates duplicate records in dB and returns a log
                # duplicate_log = lib.duplicate_check(potential_dupl, del_confirm_to_shipment,
                #                                     cust_confirm_to_shipment, ol_to_shipment)
                # if duplicate_log:
                #     # mydb.commit()
                #     pass
                # warning_log = warning_log.update(duplicate_log)
            if not warning_log:
                print(f'Warning log is empty - {line_count-1} records processed successfully.')
            else:
                for line in warning_log:
                    print(f'Line {line} - {warning_log[line][:20]}')
            print(f'End of invoice {inv_no}\n------------------------------------------------------------')

    if master_inv_file:
        inv_file_master(invoice_processing_time, inv_date)
    if separate_inv_file:
        inv_file_sep(invoice_processing_time, inv_date)
    if sum_file:
        account_sum_file(invoice_processing_time, inv_date)
    if disputes_file:
        dispute_file(invoice_processing_time, inv_date)
    if correct_history_accounts:
        correct_history_recs(invoice_processing_time)
    if create_profit_report:
        lib.create_profit_report()
    # print(f'{len(duplicates)} duplicates:')
    # for d in duplicates:
    #     print(d)


# invoices: 0021510238_20201206, 0021527078_20201206
# generate_bills(file, inv_file='master', sum_file=True, to_sql_mh=True, to_sql_dispute=True, manual_acc_input=False)
# generate_bills('0021529273_20201213.csv', inv_file='separate',
#                sum_file=False, to_sql_mh=False, to_sql_dispute=False)


def ftp_scan():
    # print(shipping_variance_log)
    # look for 3 files on saturday (day before invoice)
    d = date.today()
    files_date = str(d-timedelta(days=(d.toordinal()) % 7+7)).replace('-', '')
    sv, ol, mu = False, False, False
    if shipping_variance_log.get(files_date) and 'Weekly' in shipping_variance_log[files_date]:
        print('Shipping Variance - OK')
        sv = True
    if overlabeled_log.get(files_date):
        print('Overlabeled - OK')
        ol = True
    if marked_up_log.get(files_date):
        print('Marked Up Items - OK')
        mu = True
    if not (sv and ol and mu):
        return
    else:
        print('Scanning for invoices...')
    with FTP('ftp.dhlecommerce-usa.com') as ftp:
        ftp.login(user='ftpusermazesolutions', passwd='dJXk@4xh5')
        ftp.cwd('/From_GMail/Invoice')
        files = []
        ftp.dir("-t", files.append)
        del files[-1]
        new_files = {}
        kwargs = {}
        count = 0
        new_files_num = len(files) - len(invoices)
        if new_files_num > 0:
            for file in files:
                if file[-4:] == '.csv':
                    file_det = file.split(' ')
                    name = file_det[-1]
                    print(name)
                    new_files[name] = f'{datetime.strptime(name[-12:-4], "%Y%m%d").strftime("%m/%d/%Y")} {file_det[-2]}'
                    kwargs[f'file_{str(count)}'] = name
                    with open(rf'invoices\{name}', 'wb') as fp:
                        ftp.retrbinary(f'RETR {name}', fp.write)
                    count += 1
                    new_files_num -= 1
                    if new_files_num == 0:
                        invoices.update(new_files)
                        with open(r'invoices\invoices.json', 'w') as f:
                            json.dump(invoices, f, indent=4)
                        break

            if kwargs:
                kwargs.update({'separate_inv_file': True, 'master_inv_file': True, 'sum_file': True,
                               'disputes_file': True, 'to_sql_mh': True, 'to_sql_dispute': True, 'create_profit_report': True,
                               'check_duplicates': True, 'correct_history_accounts': True, 'manual_acc_input': False})
                # print(kwargs)

                generate_bills(**kwargs)
            #
        else:
            print(f'No new files found at {datetime.now().strftime("%m/%d/%Y %H:%M:%S")} scan')


# krgs = {'file_0': r'0021545731_20210131.csv', 'separate_inv_file': True, 'master_inv_file': True,
#         'sum_file': True, 'disputes_file': True, 'to_sql_mh': True, 'to_sql_dispute': True,
#         'create_profit_report': True, 'check_duplicates': True, 'manual_acc_input': False}

if __name__ == "__main__":
    krgs = {'file_0': r'0021557190_20210228.csv', 'separate_inv_file': True, 'master_inv_file': True,
            'sum_file': True, 'disputes_file': True, 'to_sql_mh': False, 'to_sql_dispute': False,
            'create_profit_report': True, 'check_duplicates': True, 'correct_history_accounts': True, 'manual_acc_input': False}
    # process_emails()
    generate_bills(**krgs)
    # ftp_scan()
    start_schedule = False
    if start_schedule:
        for hour in range(0, 24):
            schedule.every().sunday.at(f'{str(hour) if hour >= 10 else "0"+str(hour)}:00').do(process_emails)
            schedule.every().sunday.at(f'{str(hour) if hour >= 10 else "0"+str(hour)}:00').do(ftp_scan)
        for hour in range(0, 24, 4):
            schedule.every().monday.at(f'{str(hour) if hour >= 10 else "0"+str(hour)}:00').do(process_emails)
            schedule.every().monday.at(f'{str(hour) if hour >= 10 else "0"+str(hour)}:00').do(ftp_scan)

        while True:
            count = 0
            schedule.run_pending()
            time.sleep(60)

#     file = open('invoices/' + file_name, 'wb')
# # file = open('invoices/' + last_file, 'wb')
#     ftp.retrbinary('RETR ' + file_name, file.write)

# list_of_files = glob.glob('*.csv')  # * means all if need specific format then *.csv
# print(list_of_files)
