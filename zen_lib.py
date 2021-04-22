try:
    with open(r'dependencies\zones\DHL 2019 zip to zone.pkl', 'rb') as f:
        dhl_zip_zone_2019 = pickle.load(f)
except FileNotFoundError:
    pass
try:
    with open(r'dependencies\zones\DHL 2020 zip to zone.pkl', 'rb') as f:
        dhl_zip_zone_2020 = pickle.load(f)
except FileNotFoundError:
    pass
try:
	with open(r'dependencies\zones\country_to_code.json', 'r') as f:
		country_to_code = json.load(f)
except FileNotFoundError:
    pass


class CarrierCharge:
    if os.path.exists(r'dependencies\charges_by_zone\carrier_charges111.pkl'):
        with open(r'dependencies\charges_by_zone\carrier_charges111.pkl', 'rb') as f:
            map = pickle.load(f)
    else:
        map = {}

    def __init__(self, carrier, location, date, service_code, ship_zone, weight, charge):
        self.carrier = carrier
        self.location = location
        self.date = date
        self.service_code = service_code
        self.ship_zone = ship_zone
        self.weight = weight
        self.charge = charge
        if CarrierCharge.map.get(carrier) is None:
            CarrierCharge.map[carrier] = {}
        if CarrierCharge.map[carrier].get(location) is None:
            CarrierCharge.map[carrier][location] = {}
        if CarrierCharge.map[carrier][location].get(date) is None:
            CarrierCharge.map[carrier][location][date] = {}
        if CarrierCharge.map[carrier][location][date].get(service_code) is None:
            CarrierCharge.map[carrier][location][date][service_code] = {}
        if CarrierCharge.map[carrier][location][date][service_code].get(ship_zone) is None:
            CarrierCharge.map[carrier][location][date][service_code][ship_zone] = {}
        # if CarrierCharge.map[carrier][location][date][service_code][ship_zone].get(weight) is None:
        CarrierCharge.map[carrier][location][date][service_code][ship_zone][weight] = charge

    def __str__(self):
        return (f"Carrier: {self.carrier}")

    def carrier(self):
        return self.carrier

    def location(self):
        return self.location

    def date(self):
        return self.date

    def service_code(self):
        return self.service_code

    def ship_zone(self):
        return self.ship_zone

    def weight(self):
        return self.weight

    def charge(self):
        return self.charge

    def update(carrier, file):
        with open(file, mode='r') as f:
            reader = csv.reader(f)
            next(reader)
            for row in reader:
                if row[2] == '83':
                    row[2] = 82
                elif row[2] == '36':
                    row[2] = 81
                elif row[2] == '382':
                    row[2] = 383
                if row[1] == '15.99':
                    row[1] = 15.999
                if not row[4].replace('.', '', 1).isdigit():
                    continue
                d = row[0]
                if d.count('-') == 2:
                    d = str(datetime.strptime(d, '%Y-%m-%d').date())
                elif d.count('/') == 2:
                    d = str(datetime.strptime(d, '%m/%d/%Y').date())
                if d == date(2021, 7, 24):
                    d = date(2021, 1, 24)
                zone = row[3]
                loc = 'domestic'
                if zone[:4] != 'USPS':
                    loc = 'international'
                    if not (row[2] == '60' and zone[:2] == 'CA'):
                        zone = zone[:2]
                    else:
                        zone = zone[:2]+zone[3]
                CarrierCharge(carrier, loc,
                              d, int(row[2]), zone, float(row[1]),
                              float(row[4]))

                # if row[2] == 70:
                #     if float(row[1]) > 400:
                #         CarrierCharge(carrier, "domestic" if row[3][:4] == "USPS" else "international",
                #                       datetime.strptime(row[0], '%m/%d/%Y').date(), 81, row[3], float(row[1]),
                #                       float(row[4]) if row[4].replace('.', '', 1).isdigit() else -2)
                #         CarrierCharge(carrier, "domestic" if row[3][:4] == "USPS" else "international",
                #                       datetime.strptime(row[0], '%m/%d/%Y').date(), 82, row[3], float(row[1]),
                #                       float(row[4]) if row[4].replace('.', '', 1).isdigit() else -2)
                #         if int(row[3][-2:]) <= 8:
                #             CarrierCharge(carrier, "domestic" if row[3][:4] == "USPS" else "international",
                #                           datetime.strptime(row[0], '%m/%d/%Y').date(), 631, row[3], float(row[1]),
                #                           float(row[4]) if row[4].replace('.', '', 1).isdigit() else -2)
        with open(r'dependencies\charges_by_zone\carrier_charges111.pkl', 'wb') as f:
            pickle.dump(CarrierCharge.map, f, pickle.HIGHEST_PROTOCOL)

    def last_active_date(carrier, location, date):
        dates = sorted(list(CarrierCharge.map[carrier][location]))
        i_l = 0
        i_r = len(dates)-1
        if date >= dates[-1]:
            return dates[-1]
        elif date < dates[0]:
            return None
        while i_l <= i_r:
            m = (i_r+i_l)//2
            if date > dates[m]:
                i_l = m+1
            elif date < dates[m]:
                i_r = m-1
            else:
                return dates[m]
        if date >= dates[m]:
            return dates[m]
        else:
            return dates[m-1]

    def charge_validate(carrier, location, date, service_code, ship_zone, weight):
        if CarrierCharge.map.get(carrier) is None:
            # print(f"Carrier '{carrier}' not found in record.")
            return None
        if CarrierCharge.map[carrier].get(location) is None:
            print(f"Location '{location}' not found in record.")
            return None
        last_active_date = CarrierCharge.last_active_date(carrier, location, date)
        if last_active_date is None:
            print(f"Date '{date}' is earlier than our earliest active date.")
            return None
        if CarrierCharge.map[carrier][location][last_active_date].get(service_code) is None:
            print(f"Service code '{service_code}' not found.", carrier, location, date, service_code, ship_zone, weight)
            return None
        if CarrierCharge.map[carrier][location][last_active_date][service_code].get(ship_zone) is None:
            print(f"Ship zone '{ship_zone}' not found.")
            return None
        return last_active_date

    def charge_rate(carrier, location, date, service_code, ship_zone, weight):
        date = str(date)
        last_active_date = CarrierCharge.charge_validate(carrier, location, date, service_code, ship_zone, weight)
        if last_active_date is None:
            print(f"Active date prior to {date} could not be found for - {carrier} - {location}")
            return
        weight_charge = CarrierCharge.map[carrier][location][last_active_date][service_code][ship_zone]
        weights = sorted(list(weight_charge.keys()))
        i_l = 0
        i_r = len(weights)-1
        if weights == []:
            print(
                f"Charge not found based on given information: {carrier, location, date, service_code, ship_zone, weight}.")
            return -2
        if weight > weights[-1]:
            print(f"Weight larger than max weight bracket: {weights[-1]}.")
            return -3
        while i_l <= i_r:
            m = (i_r+i_l)//2
            if weight > weights[m]:
                i_l = m+1
            elif weight < weights[m]:
                i_r = m-1
            else:
                return weight_charge[weights[m]]
        if weight <= weights[m]:
            return weight_charge[weights[m]]
        else:
            return weight_charge[weights[m+1]]

    def charge_weight(carrier, location, date, service_code, ship_zone, weight):
        if not weight:
            return weight
        last_active_date = CarrierCharge.charge_validate(carrier, location, date, service_code, ship_zone, weight)
        if last_active_date is None:
            print(f"Active date prior to {date} could not be found for - {carrier} - {location}")
            return
        weight_charge = CarrierCharge.map[carrier][location][last_active_date][service_code][ship_zone]
        weights = sorted(list(weight_charge.keys()))
        i_l = 0
        i_r = len(weights)-1
        if weights == []:
            print(
                f"Charge not found based on given information: {carrier, location, date, service_code, ship_zone, weight}.")
            return -2
        if weight > weights[-1]:
            print(f"Weight larger than max weight bracket: {weights[-1]}.")
            return -3
        while i_l <= i_r:
            m = (i_r+i_l)//2
            if weight > weights[m]:
                i_l = m+1
            elif weight < weights[m]:
                i_r = m-1
            else:
                return weights[m]
        if weight <= weights[m]:
            return weights[m]
        else:
            return weights[m+1]


class Customer:
    if os.path.exists(r'customers.json'):
        with open(r'customers.json', 'r') as f:
            crm = json.load(f)
    else:
        crm = {}
    mydb = mysql.connector.connect(
        **db_cred
    )
    print(mydb)
    mycursor = mydb.cursor()

    def __init__(self, account_no, mailer_id=None, location=None, date=None, tier=None, payment_method=None, fee=None, name=None, qb_client=None):
        account_no = str(account_no)
        self.account_no = account_no
        self.name = name
        self.mailer_id = mailer_id
        self.location = location
        date = str(date) if date else None
        if date:
            if date.count('-') == 2:
                date = datetime.strptime(date, '%Y-%m-%d').strftime('%Y-%m-%d')
            elif date.count('/') == 2:
                date = datetime.strptime(date, '%m/%d/%Y').strftime('%Y-%m-%d')
        self.date = date
        self.tier = tier
        self.payment_method = payment_method
        self.fee = fee
        self.qb_client = qb_client
        if account_no not in Customer.crm:
            Customer.crm[account_no] = {"mailer id": mailer_id, "tiers": {
                "domestic": [], "international": []}, 'payment method': payment_method, 'fee': fee}
        else:
            print(f'{name} already in system.')
            return
        if location and date and tier:
            bisect.insort(Customer.crm[account_no]["tiers"][location], [date, tier])
        if name:
            Customer.crm[account_no]['name'] = name
        if payment_method:
            Customer.crm[account_no]['payment method'] = payment_method
        if fee or fee == 0:
            Customer.crm[account_no]['fee'] = fee
        if qb_client:
            Customer.crm[account_no]['qb client'] = qb_client
        with open(r'customers.json', 'w') as f:
            json.dump(Customer.crm, f, indent=4)
        sql_insert = 'INSERT INTO customer (acc, mailer_id, payment_method, fee, name, qb_client) \
            VALUES (%s, %s, %s, %s, %s, %s)'
        Customer.mycursor.execute(sql_insert, (account_no, mailer_id, payment_method, fee, name, qb_client))
        Customer.mydb.commit()

    def __str__(self):
        return f"Account #: {self.account_no}:\n{Customer.crm[self.account_no]}"

    def account_no(self):
        return self.account_no

    def mailer_id(self):
        return self.mailer_id

    def tiers(self):
        return Customer.crm[self.account_no]["tiers"]

    def update(account_no, location=None, rate_date=None, tier=None, mailer_id=None, fee=None, payment_method=None, name=None, qb_client=None):
        account_no = str(account_no)
        if account_no not in Customer.crm:
            Customer(account_no)
        rate_date = str(rate_date) if rate_date else rate_date
        if rate_date and rate_date.count('-') == 2:
            rate_date = datetime.strptime(rate_date, '%Y-%m-%d').strftime('%Y-%m-%d')
        elif rate_date and rate_date.count('/') == 2:
            rate_date = datetime.strptime(rate_date, '%m/%d/%Y').strftime('%Y-%m-%d')
        if tier and location and rate_date:
            bisect.insort(Customer.crm[account_no]["tiers"][location], [rate_date, tier])
        sql_params = {}
        if mailer_id:
            Customer.crm[account_no]['mailer id'] = mailer_id
            sql_params[mailer_id] = 'mailer_id = %s'
        if fee or fee == 0:
            Customer.crm[account_no]['fee'] = fee
            sql_params[fee] = 'fee = %s'
        if payment_method:
            Customer.crm[account_no]['payment method'] = payment_method
            sql_params[payment_method] = 'payment_method = %s'
        if name:
            Customer.crm[account_no]['name'] = name
            sql_params[name] = 'name = %s'
        if qb_client:
            Customer.crm[account_no]['qb client'] = qb_client
            sql_params[qb_client] = 'qb_client = %s'
        with open(r'customers.json', 'w') as f:
            json.dump(Customer.crm, f, indent=4)
        if sql_params:
            sql = 'UPDATE customer SET '
            sql += ', '.join(list(sql_params.values())) + ' WHERE acc = %s'
            sql_vals = list(sql_params.keys())+[int(account_no)]
            print(sql)
            print(sql_vals)
            Customer.mycursor.execute(sql, sql_vals)
            Customer.mydb.commit()

    def update_bulk(file):
        with open(file, 'r', encoding='unicode_escape') as f:
            reader = csv.reader(f)
            next(reader)
            for row in reader:
                for i, v in enumerate(row):
                    if not v:
                        row[i] = None
                    elif v.isnumeric():
                        row[i] = int(row[i])
                    elif v.replace('.', '', 1).isnumeric():
                        row[i] = float(row[i])
                name, mailer_id, account_no, fee, payment_method = row[:2]+row[3:6]
                account_no = str(account_no)
                if account_no not in Customer.crm:
                    print(f'Customer {row[0]} not in system. Adding customer to system.')
                    Customer(account_no)
                if row[6]:
                    Customer.update(account_no=account_no, mailer_id=mailer_id,
                                    fee=fee, payment_method=payment_method, name=name)
                    for i, v in enumerate(row[6:-2:3]):
                        if v:
                            if v.count('-') == 2:
                                rate_date = datetime.strptime(v, '%Y-%m-%d').strftime('%Y-%m-%d')
                            elif v.count('/') == 2:
                                rate_date = datetime.strptime(v, '%m/%d/%Y').strftime('%Y-%m-%d')
                            else:
                                continue
                        if row[i*3+7]:
                            # recent_tier(account_no, 'domestic')
                            recent_tier_date = Customer.crm[account_no]['tiers']['domestic'][-1][0] if Customer.crm[account_no]['tiers']['domestic'] else None
                            print(recent_tier_date, rate_date)
                            if recent_tier_date and rate_date <= recent_tier_date:
                                print(
                                    f'Trying to add domestic {rate_date} update to {account_no} where {recent_tier_date} exists. Skipping update.')
                            else:
                                tier = row[i*3+7]
                                Customer.update(account_no=account_no,
                                                location='domestic', rate_date=rate_date, tier=tier)
                                print(account_no, 'domestic', rate_date, tier, 'updated successfully.')
                        if row[i*3+8]:
                            recent_tier_date = Customer.crm[account_no]['tiers']['international'][-1][0] if Customer.crm[account_no]['tiers']['international'] else None
                            if recent_tier_date and rate_date <= recent_tier_date:
                                print(
                                    f'Trying to add international {rate_date} update to {account_no} where {recent_tier_date} exists. Skipping update.')
                            else:
                                tier = row[i*3+8]
                                Customer.update(account_no=account_no,
                                                location='international', rate_date=rate_date, tier=tier)
                                print(account_no, 'international', rate_date, tier, 'updated successfully.')

    def delete(account_no):
        account_no = str(account_no)
        c = Customer.crm.pop(account_no, None)
        if not c:
            print(f'Customer # {account_no} not found in our system.')
        else:
            with open(r'customers.json', 'w') as f:
                json.dump(Customer.crm, f, indent=4)
            Customer.mycursor.execute('DELETE FROM customer WHERE acc = %s', (account_no, ))
            Customer.mydb.commit()
            print(f'Customer # {account_no} successfully deleted.')

    def recent_tier(account_no, location, ship_date=str(date.today())):
        account_no = str(account_no)
        if ship_date.count('-') == 2:
            ship_date = datetime.strptime(ship_date, '%Y-%m-%d').strftime('%Y-%m-%d')
        elif ship_date.count('/') == 2:
            ship_date = datetime.strptime(ship_date, '%m/%d/%Y').strftime('%Y-%m-%d')
        dates_to_rates = Customer.crm[account_no]["tiers"][location]
        # dates = list(dates_to_rates.keys())
        i_l = 0
        i_r = len(dates_to_rates)-1
        if not dates_to_rates or ship_date < dates_to_rates[0][0]:
            return None
        if ship_date >= dates_to_rates[-1][0]:
            return dates_to_rates[-1][1]
        while i_l <= i_r:
            m = (i_r+i_l)//2
            if ship_date > dates_to_rates[m][0]:
                i_l = m+1
            elif ship_date < dates_to_rates[m][0]:
                i_r = m-1
            else:
                return dates_to_rates[m][1]
        if ship_date >= dates_to_rates[m][0]:
            return dates_to_rates[m][1]
        else:
            return dates_to_rates[m-1][1]

    def outdated_recent_tier_accounts(date):
        outdated = []
        for c in Customer.crm:
            if 'domestic' in Customer.crm[c]['tiers'] and Customer.crm[c]['tiers']['domestic'] and Customer.recent_tier(c, 'domestic')[0] < date:
                recent_tier = Customer.recent_tier(c, 'domestic')
                outdated.append((c, Customer.crm[c]['name'], 'domestic', recent_tier[0], recent_tier[1]))
            if 'international' in Customer.crm[c]['tiers'] and Customer.crm[c]['tiers']['international'] and Customer.recent_tier(c, 'international')[0] < date:
                recent_tier = Customer.recent_tier(c, 'international')
                outdated.append((c, Customer.crm[c]['name'], 'international', recent_tier[0], recent_tier[1]))
        if outdated:
            return outdated
        return None

