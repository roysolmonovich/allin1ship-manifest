import pandas as pd
from numpy import nan
from c import db_cred
from datetime import date, datetime
import mysql.connector
sql_insert = 'INSERT INTO transactions \
(trans_id, date, alias, card_type, last_4, credit_debit, status, amount, fee)\
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)'
mydb = mysql.connector.connect(
    **db_cred
)
print(mydb)
mycursor = mydb.cursor()
print(mycursor)


def insert_data(row):
    is_card = True if row['Cardholder Name'] is not nan else False
    print(row['Date'], type(row['Date']))
    trans_date = datetime.strptime(row['Date'].split(' ', 1)[0], '%m/%d/%Y').date()
    print(trans_date)
    alias = row['Cardholder Name'] if is_card else f'{row["Payor First Name"]} {row["Payor Last Name"]}'
    card_type = row['Card'] if is_card else None
    last_4 = row['Card No'] if is_card else None
    credit_debit = None
    if row['Credit/Debit'] == 'Credit':
        credit_debit = True
    elif row['Credit/Debit'] == 'Debit':
        credit_debit = False
    amount = float(row['Amount'].replace(',', '').strip('$()'))
    fee = float(row['Fee'].replace(',', '').strip('$()')) if row['Fee'] is not nan else None
    print(amount, fee)
    try:
        mycursor.execute(sql_insert, (row['Trans ID'], trans_date, alias, card_type,
                                      last_4, credit_debit, row['Status'], amount, fee))
    except mysql.connector.errors.IntegrityError:
        print('Credit memo already in system. Trying to continue...')


def qb_invoice(file):
    df = pd.read_excel(file)
    pd.set_option('display.max_columns', None)
    print(df.head())
    # df.apply(insert_data, axis=1)


qb_invoice(r'C:\users\Roy Solmonovich\Downloads\sales (20).xls')
