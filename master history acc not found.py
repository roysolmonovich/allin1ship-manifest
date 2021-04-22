import mysql.connector
import lib
from datetime import datetime, date, timedelta
from main import inv_file_sep, inv_file_master, account_sum_file
mydb = mysql.connector.connect(
    host="162.241.219.134",
    user="allinoy4_user0",
    password="+3mp0r@ry",
    database="allinoy4_allin1ship"
)
print(mydb)
mycursor = mydb.cursor()
# def acc_num(bol, deliv_confirm, zone, ol, cust_conf, int_tr_no, zip):
# sql = "SELECT bol_number, pricing_zone, overlabeled_value, customer_confirm, \
# internal_tracking, recipient_zip, delivery_confirm, ship_date, material_or_vas_num, weight_oz \
# FROM dhl_master_invoice \
# WHERE account < 0"


def correct_history_accounts(invoice_processing_time):
    d = date.today()
    bill_date = str(d-timedelta(days=d.toordinal() % 7))  # Current week's Sunday
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

    print(len(dic1))
    if not dic1:
        print('No new accounts to correct found...')
        return
    sql_update1 = 'UPDATE dhl_master_invoice \
    SET account = %s, charge = %s \
    WHERE ship_date = %s \
    AND delivery_confirm = %s \
    AND material_or_vas_num = %s \
    AND account < 0;'
    mycursor.executemany(sql_update1, mh_vals_bulk)
    invoice_processing_time = datetime.now().strftime('%y%m%d %H %M %S')
    sql_inv = 'SELECT `account`, `Ship Date`, `Customer Reference`, `Customer Confirmation`, \
            `Delivery Confirmation`, `Recipient Address`, `Service`, `Weight (oz)`, `Pricing Zone`, \
            `USPS Comp. Rate`, `Charge`, `Surcharges`, `Total Savings`, `Total Charges` \
            FROM invoice_view WHERE (RIGHT(`Delivery Confirmation`, LENGTH(`Delivery Confirmation`)-1), `Ship Date`, `Service`) IN ((%s, %s, %s))'
    mycursor.executemany(sql_inv, list(dic2.keys()))
    print(mycursor.fetchall())
    sql_acc_sum = 'SELECT account, SUM(charge), SUM(surcharges), SUM(charge+surcharges), SUM(usps_comp_rate), \
            SUM(usps_comp_rate-charge), SUM(total_charges_dhl) \
            FROM dhl_master_invoice JOIN invoices ON invoice = inv_no \
            WHERE (dhl_master_invoice.delivery_confirm, dhl_master_invoice.ship_date, dhl_master_invoice.material_or_vas_num) IN '\
            + str(tuple(dic1.keys())) + 'GROUP BY account'
    mycursor.execute(sql_acc_sum)
    print(mycursor.fetchall())
    inv_file_sep(invoice_processing_time, sql=sql_inv, history_corrected=True)
    inv_file_master(invoice_processing_time, sql=sql_inv, history_corrected=True)
    account_sum_file(invoice_processing_time, sql=sql_acc_sum, history_corrected=True)
    corrected_count_sql = 'SELECT COUNT(*) FROM dhl_master_invoice WHERE bill_date IS NULL and (delivery_confirm, ship_date, material_or_vas_num) IN ((%s, %s, %s))'
    mycursor.executemany(corrected_count_sql, list(dic1.keys()))
    count_before = mycursor.fetchone()
    sql_update2 = f"UPDATE dhl_master_invoice \
        SET bill_date = {bill_date} WHERE (delivery_confirm, ship_date, material_or_vas_num) IN ((%s, %s, %s))"
    mycursor.executemany(sql_update2, list(dic1.keys()))
    mydb.commit()
    mycursor.executemany(corrected_count_sql, list(dic1.keys()))
    count_after = mycursor.fetchone()
    print(f'{count_before}-{count_after} records corrected.')


# bill_date = d-timedelta(days=d.toordinal() % 7)
bill_date = date(2021, 2, 14)
print("%01d/%01d/%d" % (bill_date.month, bill_date.day, bill_date.year))
# sql_check_dispute = 'SELECT COUNT(*) FROM to_dispute \
# WHERE (delivery_confirm, ship_date, material_or_vas_num) IN '+str(tuple(dic1.keys()))
# mycursor.execute(sql_check_dispute)
# print(mycursor.fetchone())
# print(sql_check_dispute)
