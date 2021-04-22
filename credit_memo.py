import csv
from datetime import datetime
import mysql.connector
from c import db_cred


def credit_memo(file):
    with open(file, encoding="unicode_escape") as csv_file:
        with mysql.connector.connect(**db_cred) as mydb:
            mycursor = mydb.cursor()
            csv_reader = csv.reader(csv_file, delimiter=',')
            row = next(csv_reader)
            sql_memo_init = 'INSERT INTO credit_memo (credit_memo, memo_date, total_issued) VALUES (%s, %s, %s)'
            sql_memo_update = 'UPDATE credit_memo \
            SET total_added_up = %s WHERE credit_memo = %s'
            sql_shipment_credit_insert = 'INSERT INTO shipment_credits (ship_date, delivery_confirm, material_or_vas_num, credit_memo, credit_amount) \
            VALUES (%s, %s, %s, %s, %s)'
            credit_memo = row[0]
            try:
                memo_date = datetime.strptime(row[1], '%Y-%m-%d').date()
            except ValueError:
                memo_date = datetime.strptime(row[1], '%m/%d/%Y').date()
            total_stated = round(float(row[2]), 2)
            total_added_up = 0
            val = (credit_memo, memo_date, total_stated)
            try:
                mycursor.execute(sql_memo_init, val)
            except mysql.connector.errors.IntegrityError:
                print('Credit memo already in system. Trying to continue...')
            for row in csv_reader:
                del_conf = row[1]
                ship_date = datetime.strptime(row[0], '%Y-%m-%d').date()
                inv_sc = int(row[2])
                key_val = [ship_date, del_conf, inv_sc]
                memo_issued = round(float(row[3]), 2)
                total_added_up += memo_issued
                try:
                    mycursor.execute(sql_shipment_credit_insert, key_val+[credit_memo, memo_issued])
                except mysql.connector.errors.IntegrityError as err:
                    if err.errno == 1452:
                        print(f'Missing dhl shipment key for {key_val}.')
                        return
                    elif err.errno == 1062:
                        print(
                            f'Dispute already exists in shipment_credits table for {key_val} associated with credit memo # {credit_memo}. Skipping record.')
            mycursor.execute(sql_memo_update, (float(total_added_up), credit_memo))
            mydb.commit()
