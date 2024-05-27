import requests
import mysql.connector
import json
import schedule
import time

# Replace 'YOUR_API_KEY' with your actual sevDesk API key
API_KEY = '07208ec9ae71951ef3a3183c47e740fb'
BASE_URL = 'https://my.sevdesk.de/api/v1'

# MySQL database configuration
MYSQL_HOST = '202.61.192.15'
MYSQL_DATABASE = 'admin_sevdesk'
MYSQL_USER = 'admin_sevdesk'
MYSQL_PASSWORD = 'Stayless92@'


def fetch_estimations():
    url = f"{BASE_URL}/Order?token={API_KEY}"
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        orders = response.json()['objects']
        # Filter orders to get only estimations (orderType == 'AN')
        estimations = [order for order in orders if order['orderType'] == 'AN']
        print(f"Fetched {len(estimations)} estimations.")
        return estimations
    else:
        print("Error details:", response.text)  # Print detailed error message
        response.raise_for_status()

def fetch_customer_details(customer_id):
    url = f"{BASE_URL}/Contact/{customer_id}?token={API_KEY}"
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        customer_details = response.json()['objects'][0]
        print(f"Fetched customer details for ID {customer_id}: {customer_details}")
        return customer_details
    else:
        print(f"Error fetching customer details for ID {customer_id}: {response.text}")
        return None

def fetch_communication_ways(contact_id):
    url = f"{BASE_URL}/CommunicationWay"
    params = {
        'token': API_KEY,
        'contact[id]': contact_id,
        'contact[objectName]': 'Contact',
        'type': 'EMAIL'
    }
    headers = {
        'Content-Type': 'application/json'
    }

    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        communication_ways = response.json()['objects']
        emails = [cw['value'] for cw in communication_ways if cw['type'] == 'EMAIL']
        print(f"Fetched emails for contact ID {contact_id}: {emails}")
        return emails
    else:
        print(f"Error fetching communication ways for contact ID {contact_id}: {response.text}")
        return []

def create_database():
    conn = mysql.connector.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE
    )
    cursor = conn.cursor()
    
    cursor.execute('''CREATE TABLE IF NOT EXISTS estimations (
                        id INT PRIMARY KEY,
                        customerId INT,
                        customerName VARCHAR(255),
                        customerEmail VARCHAR(255),
                        status VARCHAR(255),
                        createDate DATETIME,
                        updateDate DATETIME,
                        sumNet FLOAT,
                        sumGross FLOAT,
                        sumTax FLOAT,
                        currency VARCHAR(10),
                        contact JSON,
                        orderNumber VARCHAR(255),
                        orderDate DATE,
                        header TEXT,
                        headText TEXT,
                        footText TEXT,
                        address TEXT,
                        customerInternalNote TEXT,
                        surename VARCHAR(255),
                        familyname VARCHAR(255),
                        name2 VARCHAR(255),
                        category JSON,
                        defaultCashbackTime INT,
                        defaultCashbackPercent FLOAT,
                        taxNumber VARCHAR(255),
                        excemptVat BOOLEAN,
                        defaultTimeToPay INT,
                        bankNumber VARCHAR(255),
                        birthday DATETIME,
                        vatNumber VARCHAR(255),
                        defaultDiscountAmount FLOAT,
                        defaultDiscountPercentage BOOLEAN,
                        gender VARCHAR(50),
                        academicTitle VARCHAR(255),
                        description TEXT,
                        title VARCHAR(255),
                        parent JSON,
                        customerNumber VARCHAR(255),
                        bankAccount VARCHAR(255)
                    )''')
    
    conn.commit()
    cursor.close()
    conn.close()

def clean_data(data):
    if not data:
        return {}
    return {k: (v if v is not None else "") for k, v in data.items()}

def insert_estimations(estimations):
    conn = mysql.connector.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE
    )
    cursor = conn.cursor()
    
    for estimation in estimations:
        contact = clean_data(estimation.get('contact', {}))
        customer_id = contact.get('id')
        customer_details = clean_data(fetch_customer_details(customer_id)) if customer_id else {}
        contact_emails = fetch_communication_ways(customer_id) if customer_id else []
        customer_email = contact_emails[0] if contact_emails else ''

        print(f"Processing estimation ID {estimation.get('id')} with customer email: {customer_email}")

        # Combine estimation and customer details into one dictionary
        data = {
            'id': estimation.get('id'),
            'customerId': customer_id,
            'customerName': customer_details.get('name', ''),
            'customerEmail': customer_email,
            'status': estimation.get('status', ''),
            'createDate': estimation.get('create', ''),
            'updateDate': estimation.get('update', ''),
            'sumNet': estimation.get('sumNet', 0.0),
            'sumGross': estimation.get('sumGross', 0.0),
            'sumTax': estimation.get('sumTax', 0.0),
            'currency': estimation.get('currency', ''),
            'contact': json.dumps(contact),
            'orderNumber': estimation.get('orderNumber', ''),
            'orderDate': estimation.get('orderDate', ''),
            'header': estimation.get('header', ''),
            'headText': estimation.get('headText', ''),
            'footText': estimation.get('footText', ''),
            'address': estimation.get('address', ''),
            'customerInternalNote': estimation.get('customerInternalNote', ''),
            'surename': customer_details.get('surename', ''),
            'familyname': customer_details.get('familyname', ''),
            'name2': customer_details.get('name2', ''),
            'category': json.dumps(customer_details.get('category', {})),
            'defaultCashbackTime': customer_details.get('defaultCashbackTime', 0),
            'defaultCashbackPercent': customer_details.get('defaultCashbackPercent', 0.0),
            'taxNumber': customer_details.get('taxNumber', ''),
            'excemptVat': customer_details.get('exemptVat', '0') == '1',
            'defaultTimeToPay': customer_details.get('defaultTimeToPay', 0),
            'bankNumber': customer_details.get('bankNumber', ''),
            'birthday': customer_details.get('birthday', None),
            'vatNumber': customer_details.get('vatNumber', ''),
            'defaultDiscountAmount': customer_details.get('defaultDiscountAmount', 0.0),
            'defaultDiscountPercentage': customer_details.get('defaultDiscountPercentage', '0') == '1',
            'gender': customer_details.get('gender', ''),
            'academicTitle': customer_details.get('academicTitle', ''),
            'description': customer_details.get('description', ''),
            'title': customer_details.get('titel', ''),
            'parent': json.dumps(customer_details.get('parent', {})),
            'customerNumber': customer_details.get('customerNumber', ''),
            'bankAccount': customer_details.get('bankAccount', '')
        }

        # Generate dynamic SQL query
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['%s'] * len(data))
        update_clause = ', '.join([f"{key}=VALUES({key})" for key in data.keys()])
        
        sql = f'''INSERT INTO estimations ({columns})
                  VALUES ({placeholders})
                  ON DUPLICATE KEY UPDATE {update_clause}'''
        
        # Debugging output for SQL query and parameters
        print(f"SQL: {sql}")
        print(f"Parameters: {list(data.values())}")

        cursor.execute(sql, list(data.values()))

    conn.commit()
    cursor.close()
    conn.close()

def job():
    print("Starting job...")
    estimations = fetch_estimations()
    insert_estimations(estimations)
    print("Job completed.")

def main():
    create_database()
    schedule.every(1).minutes.do(job)  # Schedule the job every 10 minutes

    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == '__main__':
    main()
