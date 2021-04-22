from lib import Customer, CarrierCharge, update_amp, del_con_to_dims, tracking_to_acc, overlabeled_report_update
import pprint
from datetime import date, datetime, timedelta
import credit_memo
from os import path, listdir
import pickle
from shutil import copyfile
from pyfiglet import figlet_format
from termcolor import cprint
import zenventory
import mysql.connector
from c import db_cred
import schedule
import time
from main_invoice import process_emails, ftp_scan, generate_bills, scheduler
dir_path = path.dirname(path.realpath(__file__))


def interface():
    cprint(figlet_format('All in 1 Ship\ninterface'))
    while True:
        print('\nWhat would you like to do?\n')
        print('Enter (1) to view customers\nEnter (2) to add a customer\nEnter (3) to update a customer')
        print('Enter (4) to delete a customer\nEnter (5) to add a carrier/tier rate file\nEnter (6) to inspect and delete a carrier/tier rate file')
        print('Enter (7) to upload a credit memo\nEnter (8) to run Zenventory app\nEnter (9) for invoice functions')
        print('Enter (10) to perform backup related operations')
        raw_input = input('\n')
        if not raw_input.isnumeric() or int(raw_input) not in range(1, 11):
            print('Invalid entry. Please enter a number between 1 and 10.')
            continue
        raw_input = int(raw_input)
        if raw_input == 1:
            # view customers
            while True:
                selection = input(
                    "\nEnter 'All' to view all customers, enter an account number to view, or enter 'Back' to return to main menu.\n").upper()
                if selection == 'ALL':
                    pprint.pprint(Customer.crm)
                elif selection == 'BACK':
                    break
                else:
                    pprint.pprint(Customer.crm.get(selection))
        elif raw_input == 2:
            # add customers
            input_dict = {}
            input_dict['account_no'] = input('\nEnter account # to insert:\n\n')
            while input_dict['account_no'] in Customer.crm:
                print('\nAccount # already exists. Try again.')
                input_dict['account_no'] = input('\nEnter account # to insert:\n\n')
            # account_no, mailer_id=None, location=None, date=None, tier=None, payment_method=None, fee=None, name=None, qb_client=None
            mailer_id = input('\nEnter a mailer ID #:\n\n')
            while not mailer_id.isnumeric():
                print('Invalid mailer ID entered - must be an integer. Try again.')
                mailer_id = input('\nEnter a mailer ID #:\n\n')
            input_dict['mailer_id'] = int(mailer_id)
            location_1 = input(
                '\nEnter a tier location - (d) for domestic or (i) for international. To skip, leave empty:\n\n')
            while location_1 not in ('', 'd', 'i'):
                print("\nInvalid entry. You must enter (d) or (i)")
                location_1 = input(
                    '\nEnter a tier location - (d) for domestic or (i) for international. To skip, leave empty:\n\n')
            if location_1:
                location_1 = 'international' if location_1.upper() == 'I' else 'domestic'
                tier_1 = input(f'\nEnter a carrier or tier for {location_1}:\n\n').upper()
                if tier_1.isnumeric():
                    tier_1 = int(tier_1)
                while tier_1 not in CarrierCharge.map:
                    print('Carrier or tier not found in system. The existing ones are listed below:')
                    for tier in CarrierCharge.map:
                        print(tier)
                    print("\nTo force tier on customer (not recommended), enter 'force tier'. To return to main menu, enter 'Back'. Otherwise, type an existing tier, \n\
                    or add tier to system through option (5) in main menu and then set customer to that tier.\n")
                    tier_option = input().upper()
                    if tier_option == 'BACK':
                        interface()
                    elif tier_option == 'FORCE TIER':
                        break
                while True:
                    date_1 = input(f'\nEnter a starting date for {location_1} - {tier_1} in format YYYY-mm-dd:\n\n')
                    try:
                        datetime.strptime(date_1, '%Y-%m-%d')
                        break
                    except ValueError:
                        print('\nInvalid date entered. Try again.')
                input_dict.update({'location': location_1, 'date': date_1, 'tier': tier_1})
                location_2 = input(
                    '\nEnter another tier location - (d) for domestic or (i) for international. To skip, leave empty:\n\n').upper()
                while location_2 not in ('', 'D', 'I'):
                    print("Invalid entry. Enter (d) or (i)")
                    location_2 = input(
                        'Enter a tier location - (d) for domestic or (i) for international. To skip, leave empty:\n\n').upper()
                if location_2:
                    location_2 = 'international' if location_2 == 'I' else 'domestic'
                    tier_2 = input(f'Enter a carrier or tier for {location_2}:\n\n').upper()
                    if tier_2.isnumeric():
                        tier_2 = int(tier_2)
                    while tier_2 not in CarrierCharge.map:
                        print('Carrier or tier not found in system. The existing ones are listed below:')
                        for tier in CarrierCharge.map:
                            print(tier)
                        print("To force tier on customer (not recommended), enter 'force tier'. To return to main menu, enter 'Back'. Otherwise, type an existing tier, \n\
                        or add tier to system through option (5) in main menu and then set customer to that tier.\n")
                        tier_option = input().upper()
                        if tier_option == 'BACK':
                            interface()
                        elif tier_option == 'FORCE TIER':
                            break
                    while True:
                        date_2 = input(
                            f'Leave blank to use {date_1} as starting date, or enter one for {location_2} - {tier_2} in format YYYY-mm-dd:\n\n')
                        if not date_2:
                            date_2 = date_1
                            break
                        else:
                            try:
                                datetime.strptime(date_2, '%Y-%m-%d')
                                break
                            except ValueError:
                                print('Invalid date entered. Try again.')
                    input_dict.update(
                        {'location_2': location_2, 'date_2': date_1 if not date_2 else date_2, 'tier_2': tier_2})
            payment_method = input('Enter a payment method (ACH, Credit Card, etc.). To skip, leave empty:\n\n')
            if payment_method:
                input_dict['payment_method'] = payment_method
            fee = input('Enter fee. To skip, leave empty:\n')
            while fee != '' and not fee.replace('.', '', 1).lstrip('-').isnumeric():
                print('Invalid fee entered - fee must be numeric. Try again.')
                fee = input('Enter fee. To skip, leave empty:\n\n')
            if fee != '':
                input_dict['fee'] = float(fee)
            input_dict['name'] = input('Enter customer name:\n\n')
            new_customer = Customer(**input_dict)
            print('\n')
            if new_customer:
                print(new_customer)
                print('Customer added successfully!\n')
        elif raw_input == 3:
            # update customers
            input_dict = {}
            input_dict['account_no'] = input('Enter account # to update:\n\n')
            while input_dict['account_no'] not in Customer.crm:
                print('Account # not found. Try again.')
                input_dict['account_no'] = input('Enter account # to update:\n\n')
            if input('Updating tier? (Y\\n):\n\n') == 'Y':
                location = input('Enter a tier location - (d) for domestic or (i) for international:\n\n').upper()
                while location not in ('D', 'I'):
                    print("Invalid entry. Enter (d) or (i)")
                    location = input('Enter a tier location - (d) for domestic or (i) for international:\n\n').upper()
                location = 'domestic' if location == 'd' else 'international'
                tier = input(f'Enter a tier for {location}:\n\n').upper()
                if tier.isnumeric():
                    tier = int(tier)
                while tier != 'COST' and tier not in CarrierCharge.map:
                    print('Carrier or tier not found in system. The existing ones are listed below:\n')
                    for _tier in CarrierCharge.map:
                        print(_tier)
                    print('COST')
                    print(f"To force tier '{_tier}' on customer (not recommended), enter 'force tier'. To return to main menu, enter 'Back'. Otherwise, type an existing tier, \n\
                    or add tier to system through option (5) in main menu and then set customer to that tier.\n")
                    tier_option = input()
                    if tier_option == 'Back':
                        interface()
                    elif tier_option == 'force tier':
                        break
                while True:
                    _date = input(f'Enter a starting date for {location} - {tier} in format YYYY-mm-dd:\n\n')
                    try:
                        datetime.strptime(_date, '%Y-%m-%d')
                        break
                    except ValueError:
                        print('Invalid date entered. Try again.')
                input_dict.update({'location': location, 'rate_date': _date, 'tier': tier})
            mailer_id = input('Mailer ID #:\nHit enter to skip or enter a new number:\n\n')
            while mailer_id and not mailer_id.isnumeric():
                print('Invalid mailer ID entered - must be an integer. Try again.')
                mailer_id = input('Enter a mailer ID #:\n\n')
            if mailer_id:
                input_dict['mailer_id'] = int(mailer_id)
            fee = input('Fee:\nHit enter to skip or enter a new fee:\n\n')
            while fee != '' and not fee.replace('.', '', 1).lstrip('-').isnumeric():
                print('Invalid fee entered - fee must be numeric. Try again.')
                fee = input('Enter fee:\n\n')
            if fee != '':
                input_dict['fee'] = float(fee)
            name = input('Customer name:\nHit enter to skip or enter a new customer name:\n\n')
            if name:
                input_dict['name'] = name
            while True:
                new_account = input(
                    f'Replace account number:\nHit enter to skip or enter a new account number for {input_dict["account_no"]}:\n\n')
                if new_account in Customer.crm:
                    print(f'{new_account} already exists. Try again.\n')
                else:
                    break
            if new_account:
                new_account_re = input(
                    f'Are you sure you want to change account {input_dict["account_no"]} to {new_account}? Retype new account to confirm:\n\n')
                if new_account_re != new_account:
                    print('Confirmation did not match new account number.\n')
                else:
                    input_dict['new_account_no'] = new_account
            print(f'Do you want to update the customer data based on the following input? (Y\\n)\n{input_dict}\n')
            if input() == 'Y':
                Customer.update(**input_dict)
                print('Customer successfully updated!')
                print('The currect records for this customer are the following:\n')
                pprint.pprint(Customer.crm.get(new_account or input_dict['account_no']))
        elif raw_input == 4:
            # delete customers
            account_no = input('Enter account # to delete:\n\n')
            while account_no not in Customer.crm:
                print(f"Account # {account_no} not found. Try again.")
                account_no = input('Enter account # to delete:\n\n')
            print(f"\nAre you sure you want to delete the following account?\n\nAccount #: {account_no}")
            pprint.pprint(Customer.crm.get(account_no))
            delete_confirm = input(f"Reenter account # to confirm.\n\n")
            while delete_confirm != account_no:
                print('Confirmation does not match account #. Try again.\n')
                delete_confirm = input(f"Reenter account # to confirm.\n\n")
            if delete_confirm == account_no:
                Customer.delete(account_no)
        elif raw_input == 5:
            # add carrier or tier rates
            tier = input('Enter a carrier or tier to add new rates to:\n\n')
            if tier.isnumeric():
                tier = int(tier)
            if tier != 'COST' and tier not in CarrierCharge.map:
                print('Carrier or tier not found in system. The existing ones are listed below:')
                for _tier in CarrierCharge.map:
                    print(_tier)
                print('COST')
                tier_option = input(f"\nDo you want to create {tier} and add rates from a file? (Y\\n)\n\n")
                if tier_option != 'Y':
                    continue
            print('Please confirm the following:\n-Each row has a date in format YYYY-mm-dd')
            print('-Charge column has to only contain numbers, otherwise line is skipped.')
            print('FYI:\n-International zones will be stored as country codes, except for Canada zones with service 60 which will be CA1-CA4')
            print('-Services 36 and 83 will be converted to 81 and 82 respectively')
            print('-Weight bracket of 15.99 will be stored as 15.999 as invoice weights have 3 decimal places\n')
            confirm_input = input("Enter 'Confirm' to confirm\n\n")
            if confirm_input != 'Confirm':
                continue
            path_input = input(f'Enter a local absolute path or relative path to {dir_path} for the rate file:\n\n')
            while True:
                try:
                    CarrierCharge.update(tier, path_input)
                    break
                except FileNotFoundError:
                    print('File not found. Try again.')
                    path_input = input(
                        f'Enter a local absolute path or relative path to {dir_path} for the rate file:\n\n')
            print('Rates submitted successfully.')
        elif raw_input == 6:
            # inspect and delete carrier or tier rate
            while True:
                print('\nCarriers and Tiers:\n')
                for _tier in CarrierCharge.map:
                    print(_tier)
                tier = input('\nSelect a carrier or tier:\n\n')
                if tier.isnumeric():
                    tier = int(tier)
                if tier not in CarrierCharge.map:
                    print('\nCarrier or tier not found.\n')
                    break
                op = input('\nEnter (1) to inspect\nEnter (0) to delete\n\n')
                if op == '0':
                    CarrierCharge.map.pop(tier, None)
                    with open(r'dependencies\charges_by_zone\carrier_charges111.pkl', 'wb') as f:
                        pickle.dump(CarrierCharge.map, f, pickle.HIGHEST_PROTOCOL)
                    print(f'{tier} deleted successfully!')
                elif op == '1':
                    print('\nLocations:\n')
                    for loc in CarrierCharge.map[tier]:
                        print(loc)
                    loc = input(f'\nSelect a location for {tier}:\n\n')
                    if loc not in CarrierCharge.map[tier]:
                        print('\nLocation not found.\n')
                        break
                    op = input('\nEnter (1) to inspect\nEnter (0) to delete\n\n')
                    if op == '0':
                        CarrierCharge.map[tier].pop(loc, None)
                        with open(r'dependencies\charges_by_zone\carrier_charges111.pkl', 'wb') as f:
                            pickle.dump(CarrierCharge.map, f, pickle.HIGHEST_PROTOCOL)
                        print(f'{tier} - {loc} deleted successfully!')
                    elif op == '1':
                        print('\nDates:\n')
                        for _date in CarrierCharge.map[tier][loc]:
                            print(_date)
                        _date = input(f'\nSelect a date for {tier} - {loc}:\n\n')
                        if _date not in CarrierCharge.map[tier][loc]:
                            print('\nDate not found.\n')
                            break
                        op = input('\nEnter (1) to inspect\nEnter (0) to delete\n\n')
                        if op == '0':
                            CarrierCharge.map[tier][loc].pop(_date, None)
                            with open(r'dependencies\charges_by_zone\carrier_charges111.pkl', 'wb') as f:
                                pickle.dump(CarrierCharge.map, f, pickle.HIGHEST_PROTOCOL)
                            print(f'{tier} - {loc} - {_date} deleted successfully!')
                        elif op == '1':
                            print('\nServices:\n')
                            for _service in CarrierCharge.map[tier][loc][_date]:
                                print(_service)
                            service = input(f'\nSelect a service for {tier} - {loc} - {_date}:\n\n')
                            if service.isnumeric():
                                service = int(service)
                            if service not in CarrierCharge.map[tier][loc][_date]:
                                print('\nService not found.\n')
                                break
                            op = input('\nEnter (1) to inspect\nEnter (0) to delete\n\n')
                            if op == '0':
                                CarrierCharge.map[tier][loc][_date].pop(service, None)
                                with open(r'dependencies\charges_by_zone\carrier_charges111.pkl', 'wb') as f:
                                    pickle.dump(CarrierCharge.map, f, pickle.HIGHEST_PROTOCOL)
                                print(f'{tier} - {loc} - {_date} - {service} deleted successfully!')
                            elif op == '1':
                                print('\nZones:\n')
                                for _zone in CarrierCharge.map[tier][loc][_date][service]:
                                    print(_zone)
                                zone = input(f'\nSelect a zone for {tier} - {loc} - {_date} - {service}:\n\n')
                                if zone not in CarrierCharge.map[tier][loc][_date][service]:
                                    print('\nZone not found.\n')
                                    break
                                op = input('\nEnter (1) to inspect\nEnter (0) to delete\n\n')
                                if op == '0':
                                    CarrierCharge.map[tier][loc][_date][service].pop(zone, None)
                                    with open(r'dependencies\charges_by_zone\carrier_charges111.pkl', 'wb') as f:
                                        pickle.dump(CarrierCharge.map, f, pickle.HIGHEST_PROTOCOL)
                                    print(f'{tier} - {loc} - {_date} - {service} - {zone} deleted successfully!')
                                elif op == '1':
                                    print('\nWeights -> Charges:\n')
                                    for _weight, charge in CarrierCharge.map[tier][loc][_date][service][zone].items():
                                        print(f'{_weight} -> {charge}')
                                    weight = input(
                                        f'\nSelect a weight for {tier} - {loc} - {_date} - {service} - {zone}:\n\n')
                                    if weight.replace('.', '', 1).isnumeric():
                                        weight = float(weight)
                                    if weight not in CarrierCharge.map[tier][loc][_date][service][zone]:
                                        print('\nWeight not found.\n')
                                        break
                                    op = input('\nEnter (0) to delete\n\n')
                                    if op == '0':
                                        CarrierCharge.map[tier][loc][_date][service][zone].pop(weight, None)
                                        with open(r'dependencies\charges_by_zone\carrier_charges111.pkl', 'wb') as f:
                                            pickle.dump(CarrierCharge.map, f, pickle.HIGHEST_PROTOCOL)
                                        print(f'{tier} - {loc} - {_date} - {service} - {zone} - {weight} deleted successfully!')
        elif raw_input == 7:
            # upload credit memo
            credit_memo_file = input(
                f'\nEnter a local absolute path or relative path to {dir_path} for the credit memo file:\n\n')
            credit_memo.credit_memo(credit_memo_file)
            print('\nCredit memo added successfully!\n')
        elif raw_input == 8:
            # zenventory
            today = date.today()
            start_date = input(
                f'\nEnter a start date in format YYYY-mm-dd or leave empty for yesterday, {today-timedelta(days=1)}\n\n') or str(today-timedelta(days=1))
            try:
                datetime.strptime(start_date, '%Y-%m-%d')
            except ValueError:
                print('\nInvalid date entered. Try again.')
                continue
            end_date = input(
                f'Enter an end date in format YYYY-mm-dd or leave empty for today, {today}\n\n') or str(today)
            try:
                datetime.strptime(end_date, '%Y-%m-%d')
            except ValueError:
                print('\nInvalid date entered. Try again.')
                continue
            write_files = input(
                f'Should updated shipments and shipments where charge was not found be written into Zenventory API folder? (Y\\n)\n\n')
            write_files = True if write_files == 'Y' else False
            input('The Zenventory task can take a few minutes to complete. To start, hit enter:\n\n')
            zenventory.update_zenventory(start_date=start_date, end_date=end_date, write_files=write_files)
            print('Zenventory task completed successfully!\n')
        elif raw_input == 9:
            # invoices
            print('\nEnter (1) to scan for and process email files\nEnter (2) to import a Shipment Variance report\nEnter (3) to import an Overlabeled report')
            print('Enter (4) to import a Customer Tracking file\nEnter (5) to import a Marked up Items report\nEnter (6) to perform an FTP scan')
            print('Enter (7) to start scheduler\nEnter (8) to process invoice(s)\nEnter (0) to delete invoice(s) from the database')
            inv_op = input('\n')
            # update_amp, del_con_to_dims, tracking_to_acc, overlabeled_report_update
            if inv_op == '0':
                del_invoices = input(
                    'Enter invoice(s) to delete. For multiple invoices, separate invoice numbers by comma:\n\n')
                invoices = tuple(del_invoices.replace(' ', '').split(','))
                mydb = mysql.connector.connect(**db_cred)
                mycursor = mydb.cursor()
                if len(invoices) == 1:
                    mycursor.execute('SELECT inv_no FROM invoices WHERE inv_no = '+invoices[0])
                else:
                    mycursor.execute('SELECT inv_no FROM invoices WHERE inv_no IN '+str(invoices))
                found_invoices = mycursor.fetchall()
                mycursor.close()
                mydb.close()
                if not found_invoices:
                    print(f'\n{"The invoice was" if len(invoices) == 1 else "The invoices were"} not found in the database.\n')
                    continue
                print('\nThe following invoices were found. Are you sure you want to delete them from the database? (Y\\n)\n')
                for i in range(len(found_invoices)):
                    found_invoices[i] = found_invoices[i][0]
                    print(found_invoices[i])
                found_invoices = tuple(found_invoices)
                if input('\n') == 'Y':
                    mydb = mysql.connector.connect(**db_cred)
                    mycursor = mydb.cursor()
                    if len(invoices) == 1:
                        mycursor.execute('DELETE FROM invoices WHERE inv_no = '+str(found_invoices[0]))
                    else:
                        mycursor.execute('DELETE FROM invoices WHERE inv_no IN '+str(found_invoices))
                    mydb.commit()
                    print(f'Invoice{"" if len(found_invoices) == 1 else "s"} deleted successfully!')
                    mycursor.close()
                    mydb.close()
                else:
                    continue
            elif inv_op == '1':
                process_emails()
            elif inv_op == '2':
                path_input = input(
                    f'Enter a local absolute path or relative path to {dir_path} for the Shipment Variance report file:\n\n')
                file_path = path_input
                if '.' not in file_path or file_path.rsplit('.', 1)[1] != '.csv':
                    file_path += '.csv'
                update_amp(file_path)
                print('Shipment Variance imported successfully!\n')
            elif inv_op == '3':
                path_input = input(
                    f'Enter a local absolute path or relative path to {dir_path} for the Overlabeled report file:\n\n')
                file_path = path_input
                if '.' not in file_path or file_path.rsplit('.', 1)[1] != '.csv':
                    file_path += '.csv'
                overlabeled_report_update(file_path)
                print('Overlabeled imported successfully!\n')
            elif inv_op == '4':
                path_input = input(
                    f'Enter a local absolute path or relative path to {dir_path} for the Customer Tracking file:\n\n')
                file_path = path_input
                if '.' not in file_path or file_path.rsplit('.', 1)[1] != '.csv':
                    file_path += '.csv'
                tracking_to_acc(file_path)
                print('Customer Tracking imported successfully!\n')
            elif inv_op == '5':
                path_input = input(
                    f'Enter a local absolute path or relative path to {dir_path} for the Marked up Items file:\n\n')
                file_path = path_input
                if '.' not in file_path or file_path.rsplit('.', 1)[1] != '.csv':
                    file_path += '.csv'
                del_con_to_dims(file_path)
                print('Marked up Items imported successfully!\n')
            elif inv_op == '6':
                ftp_scan()
            elif inv_op == '7':
                print('\nA schedule will be created to run a task once every hour on Sunday and once every four hours on Monday.')
                print('The task will scan for new email files, and when done, it will scan for invoices in the FTP.')
                print('The email files are logged by the file dates.')
                print('The actual FTP scan only begins if weekly Shipping Variance, Overlabeled, and Marked up Items reports are imported and logged.')
                print('If there are any errors while importing a file due to an issue with the file, the code will have to be modified to account for that issue.')
                print('Alternatively, the file will have to be deleted from the email, corrected, and resent, as the scan attempts to import and log all unlogged files.')
                print('The interface will be dedicated to the task schedule. Open a new command prompt to continue using the interface.')
                print('To cancel, hit CTRL-C.')
                input('To start, hit enter:\n\n')
                scheduler()
            elif inv_op == '8':
                kwargs = {}
                # {'separate_inv_file': True, 'master_inv_file': True, 'sum_file': True,
                #                'disputes_file': True, 'to_sql_mh': True, 'to_sql_dispute': True, 'create_profit_report': True,
                #                'check_duplicates': True, 'correct_history_accounts': True, 'manual_acc_input': False}
                count = 0
                while True:
                    if not count:
                        file_input = input("Enter invoice file to process (include '.csv' extension):\n\n")
                    else:
                        file_input = input(
                            "Enter invoice file to process (include '.csv' extension), or enter 'Next' if done:\n\n")
                        if file_input == 'Next':
                            break
                    kwargs[f'file_{count}'] = file_input
                    count += 1
                to_sql_mh = True if input('Add processed records to database ([Y]\\n):\n\n') in ('', 'Y') else False
                to_sql_dispute = True if input(
                    'Add disputes to database if found ([Y]\\n):\n\n') in ('', 'Y') else False
                disputes_file = True if to_sql_dispute and input(
                    'Create a file showing the created disputes ([Y]\\n):\n\n') in ('', 'Y') else False
                separate_inv_file = True if input(
                    'Create a separate invoice for each customer ([Y]\\n):\n\n') in ('', 'Y') else False
                master_inv_file = True if input(
                    'Create a master invoice file with all customer records ([Y]\\n):\n\n') in ('', 'Y') else False
                sum_file = True if input('Create a customer sum charges file ([Y]\\n):\n\n') in ('', 'Y') else False
                create_profit_report = True if input(
                    'Create a profit per customer by week pivot table: ([Y]\\n):\n\n') in ('', 'Y') else False
                check_duplicates = True if input(
                    'Check invoices for duplicates and add them to database to avoid billing ([Y]\\n):\n\n') in ('', 'Y') else False
                correct_history_accounts = True if input(
                    'Try to find accounts and charges in history where missing ([Y]\\n):\n\n') in ('', 'Y') else False
                manual_acc_input = True if input(
                    'Are the invoice(s) entered manual account input invoices (account # in last column)? (Y\\[n])\n\n') == 'Y' else False
                print(to_sql_mh, to_sql_dispute, separate_inv_file, master_inv_file, sum_file,
                      create_profit_report, check_duplicates, correct_history_accounts, disputes_file, manual_acc_input)
                kwargs.update({'to_sql_mh': to_sql_mh, 'to_sql_dispute': to_sql_dispute, 'separate_inv_file': separate_inv_file,
                               'master_inv_file': master_inv_file, 'sum_file': sum_file, 'create_profit_report': create_profit_report, 'check_duplicates': check_duplicates,
                               'correct_history_accounts': correct_history_accounts, 'disputes_file': disputes_file, 'manual_acc_input': manual_acc_input})
                generate_bills(**kwargs)

        elif raw_input == 10:
            # backup
            backup_op = input(
                'What backup operation would you like to perform?\nEnter (1) to create a backup\nEnter (2) to restore from a backup\n\n')
            if backup_op == '1':
                file_op = input(
                    'Which file would you like to backup?\nEnter (1) to backup carrier/tier rate file\nEnter (2) to backup customers info file\n\n')
                file_path = input('\nChoose a file name:\n\n')
                if file_op == '1':
                    src = r'dependencies\charges_by_zone\carrier_charges111.pkl'
                    dest = r'dependencies\charges_by_zone\charges_backup\\'
                    ext = '.pkl'
                elif file_op == '2':
                    src = r'customers.json'
                    dest = r'customers_backup\\'
                    ext = '.json'
                else:
                    print('\nInvalid selection\n')
                    continue
                copyfile(src, dest+file_path+ext)
            elif backup_op == '2':
                file_op = input(
                    'Which file would you like to restore?\nEnter (1) to restore carrier/tier rate file\nEnter (2) to restore customers info file\n\n')
                if file_op == '1':
                    dir = r'dependencies\charges_by_zone\charges_backup\\'
                    src = r'dependencies\charges_by_zone\carrier_charges111.pkl'
                    ext = '.pkl'
                elif file_op == '2':
                    dir = r'customers_backup\\'
                    src = 'customers.json'
                    ext = '.json'
                else:
                    print('\nInvalid selection\n')
                    continue
                print('')
                lstdir = listdir(dir)
                if lstdir:
                    for backup in sorted(lstdir, key=lambda x: path.getmtime(dir+x)):
                        print(backup.rsplit('.', 1)[0])
                    backup_restore = input('\nEnter backup name to restore:\n\n') + ext
                    if not path.exists(dir+backup_restore):
                        continue
                    else:
                        copyfile(dir+backup_restore, src)
                else:
                    print('No backups found\n')
                    continue
            else:
                print('Invalid selection')


if __name__ == '__main__':
    interface()
