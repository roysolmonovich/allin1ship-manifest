import csv
import pickle


def customers():
    with open(r'C:\Users\Roy Solmonovich\Desktop\Allin1Ship\Development\hashes\sheets\Customers.csv', mode='r') as infile:
        reader = csv.reader(infile)
        next(reader)
        next(reader)
        customers = {int(rows[7]): int(rows[5]) for rows in reader}
        with open(r'C:\Users\Roy Solmonovich\Desktop\Allin1Ship\Development\hashes\sheets\customers.pkl', 'wb') as f:
            pickle.dump(customers, f, pickle.HIGHEST_PROTOCOL)


def overlabeled():
    with open(r'C:\Users\Roy Solmonovich\Desktop\Allin1Ship\Development\hashes\sheets\Overlabeled.csv', mode='r') as infile:
        reader = csv.reader(infile)
        next(reader)
        overlabeled = {rows[1][1:]: (rows[3]) for rows in reader}
        with open(r'C:\Users\Roy Solmonovich\Desktop\Allin1Ship\Development\hashes\sheets\overlabeled.pkl', 'wb') as f:
            pickle.dump(overlabeled, f, pickle.HIGHEST_PROTOCOL)


def amp():
    with open(r'C:\Users\Roy Solmonovich\Desktop\Allin1Ship\Development\hashes\sheets\amp.csv', encoding="unicode_escape") \
            as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        next(csv_reader)
        line = 1
        del_to_acc = {}
        cust_to_acc = {}
        for row in csv_reader:
            line += 1
            if row[1] not in ('', '\''):
                del_to_acc[int(row[1][1:])] = row[16]
                cust_to_acc[row[0][1:]] = row[16]
        with open(r'C:\Users\Roy Solmonovich\Desktop\Allin1Ship\Development\hashes\sheets\amp.pkl', 'wb') as f:
            pickle.dump((del_to_acc, cust_to_acc), f, pickle.HIGHEST_PROTOCOL)


# # print(amp()[0].get(9361269903503789505877))
# customers()
