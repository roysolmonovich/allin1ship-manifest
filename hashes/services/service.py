import csv
import pickle
from datetime import date, datetime, timedelta
# import sys
# print('Number of arguments:', len(sys.argv), 'arguments.')
# print('Argument List:', str(sys.argv), type(sys.argv[0]))
# d0 = date.today()
# curr_wk_sun = d0-timedelta(days=d0.toordinal() % 7)
# print(curr_wk_sun)
# print(date(2020, 4, 13))

# def func(**kwargs):
#     for c in kwargs:
#         print(c, kwargs[c])
#     return
#
#
# print(func(a=1, b=2, c=3))


# x = [1.00001, 3.4588889, None, 7.4444445]
# x = [round(num, 2) if num is not None else None for num in x]
# print(x)
# add bill date - last invoice, work on SQL

# with open(r'C:\Users\Roy Solmonovich\Desktop\Allin1Ship\Development\hashes\zip to zone\DHL 2020 zip to zone.pkl', 'rb') as f:
#     dhl_zip_zone_2020 = pickle.load(f)
# print(dhl_zip_zone_2020)
# with open(r'C:\Users\Roy Solmonovich\Desktop\Allin1Ship\Development\hashes\services\dhl_service_hash.csv', encoding="unicode_escape") as infile:
#     reader = csv.reader(infile)
#     dhl_to_ai1s = {int(rows[0]): (int(rows[1]), int(rows[2]), int(rows[3]), rows[4]) for rows in reader}
# with open(r'C:\Users\Roy Solmonovich\Desktop\Allin1Ship\Development\hashes\services\dhl_service_hash.pkl', 'wb') as f:
#     pickle.dump(dhl_to_ai1s, f, pickle.HIGHEST_PROTOCOL)
# with open(r'C:\Users\Roy Solmonovich\Desktop\Allin1Ship\Development\hashes\zip to zone\DHL 2020 zip.csv', encoding="unicode_escape") as infile:
#     reader = csv.reader(infile)
#     next(reader)
#     dhl_zip_zone_2020 = {row[1]: '0' + row[2] if len(row[2]) == 1 else row[2] for row in reader}
# with open(r'C:\Users\Roy Solmonovich\Desktop\Allin1Ship\Development\hashes\zip to zone\DHL 2020 zip to zone.pkl', 'wb') as f:
#     pickle.dump(dhl_zip_zone_2020, f, pickle.HIGHEST_PROTOCOL)
# print(dhl_zip_zone_2020)
# print (dhl_to_ai1s)
# print(dhl())
