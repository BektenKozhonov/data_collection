import logging
import pandas as pd
from io import StringIO
from simple_salesforce import Salesforce
from salesforce_bulk import SalesforceBulk
from dotenv import load_dotenv
import os
import requests
from typing import Optional, List


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Загружаем переменные из .env файла
load_dotenv()


class SalesforceAuthentication:
    # Атрибуты класса для хранения сессий, общих для всех экземпляров
    sf_rest_session = None
    sf_bulk_session = None

    @classmethod
    def initialize_salesforce_session(cls):
        """Авторизуемся в Salesforce и сохраняем сессии для REST API и Bulk API для последующего использования."""
        try:
            username = os.getenv('SALESFORCE_USERNAME')
            password = os.getenv('SALESFORCE_PASSWORD')
            security_token = os.getenv('SALESFORCE_TOKEN')
            domain = os.getenv('SALESFORCE_DOMAIN')

            if not all([username, password, security_token, domain]):
                raise ValueError("One or more Salesforce authentication environment variables are missing.")


            sf = Salesforce(username=username, password=password, security_token=security_token, domain=domain)
            bulk = SalesforceBulk(sessionId=sf.session_id, host=sf.sf_instance)
            logger.info("Successfully authenticated to Salesforce.")
            cls.sf_rest_session = sf
            cls.sf_bulk_session = bulk
        except Exception as e:
            logger.error(f"Authentication error: {str(e)}")
            cls.sf_rest_session, cls.sf_bulk_session = None, None

    @classmethod
    def get_sessions(cls):
        """Возвращает текущие сессии для REST и Bulk API, инициирует новую сессию при необходимости."""
        # Проверяем, инициированы ли сессии, и если нет — инициализируем их один раз для всех экземпляров
        if not cls.sf_rest_session or not cls.sf_bulk_session:
            cls.initialize_salesforce_session()
        return cls.sf_rest_session, cls.sf_bulk_session

class BulkLoadProcessor(SalesforceAuthentication):
    def __init__(self):
        super().__init__()
        self.load_data = []

    def add_load(self, load_record):
        self.load_data.append(load_record)

    def send_bulk_data(self, text):
        df = pd.DataFrame(self.load_data)
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        self.csv_data = csv_buffer.getvalue()

        if not self.csv_data.strip():
            logger.info("No data to send.")
            return
        
        job = None
        try:
            job = self.sf_bulk_session.create_insert_job(f"{text}", contentType='CSV')
            batch = self.sf_bulk_session.post_batch(job, self.csv_data)
            logger.debug(f"Batch response: {batch}")
            self.sf_bulk_session.wait_for_batch(job, batch)
            logger.info(f"Bulk operation completed for {len(self.load_data)} records.")
        except AttributeError as e:
            logger.error(f"Method not found: {e}")
        except Exception as e:
            logger.error(f"An error occurred during bulk processing: {e}")
        finally:
            if job:
                self.sf_bulk_session.close_job(job)
            self.load_data = []



class ObjectMapper(SalesforceAuthentication):
    def __init__(self):
        super().__init__()
        self.broker_map = {}

    def get_broker_map(self, broker_name: str) -> dict:
        
        broker = self.find_broker_by_name(broker_name)
        if not broker:
            broker = self.create_broker_in_account(broker_name)

        return self.broker_map

    def find_broker_by_name(self, name):
        query = f"SELECT Id, Name FROM Account WHERE Name = '{name}'"
        result = self.sf_rest_session.query(query)
        return result['records'][0] if result['records'] else None

    def create_broker_in_account(self, name):
        if 'AMAZON' in name.upper():
            account_data = {'Name': name, 'Type': 'Broker', 'AMAZON__c': True}

        else:
            account_data = {'Name': name, 'Type': 'Broker', 'STREETLOAD__c': True, 'PROOF_OF_DELIVERY__c': True, 'RATE_CONFIRMATION__c': True}, 
        return self.sf_rest_session.Account.create(account_data)

    def get_driver_map(self, driver_name: str):

        if not driver_name:
            return driver_name
        driver_account = self.find_driver_by_name(driver_name)
        if driver_account:
            self.driver_map[driver_name] = {
                'OWN_MC__c': driver_account.get('OWN_MC__c'),
                'Id': driver_account.get('Id'),
                'DRIVER_ID__c': driver_account.get('DRIVER_ID__c')
            }
        else:
            logger.warning(f"No driver account found for name: '{driver_name}'")

        return self.driver_map

    def find_driver_by_name(self, driver_name):
        try:
            escaped_driver_name = driver_name.replace("'", "\\'").upper()
            query = f"""
            SELECT Id, Name, OWN_MC__c, DRIVER_ID__c
            FROM Account
            WHERE RecordType.DeveloperName = 'DriverAccount' AND Name = '{escaped_driver_name}'
            """
            result = self.sf_rest_session.query(query)
            records = result['records']
            if not records:
                return None
            elif len(records) > 1:
                logger.warning(f"Multiple driver accounts found for name '{driver_name}'. Using the first one.")
            return records[0]
        except Exception as e:
                logger.error(f"Error querying driver by name '{driver_name}': {e}")
                return None
        

class TripSetter(SalesforceAuthentication):
    def __init__(self, save_folder: str):
        super().__init__()
        self.save_folder = save_folder
        self.file_path = None

    def get_data_csv(self, result, num):            
    # Check if records are available
        if not result['records']:
            logger.warning("No records found for the provided load numbers")
            return None

        # Save data to CSV
        df = pd.DataFrame(result['records']).drop(columns='attributes')
        self.file_path = os.path.join(self.save_folder, f'salesforce_data{num}.csv')
        df.to_csv(self.file_path, index=False)
        logger.info(f"Data successfully saved to {self.file_path}")

        return self.file_path  # Return path of the saved file

    def making_trip_sql_request(self, load_numbers: List[str]) -> Optional[str]: # set_tripsql_request()
        """Downloads a CSV file with Trip pickup and delivery ID if it exists."""
        try:
            # Create the save folder if it doesn't exist
            os.makedirs(self.save_folder, exist_ok=True)
            
            # Ensure Salesforce session is initialized
            if not self.sf_rest_session:
                raise Exception('Salesforce REST session not initialized')

            # Prepare and execute Salesforce query
            load_numbers_str = ','.join([f"'{num}'" for num in load_numbers])
            query = f"""
                SELECT Id, Load_Number__c, (SELECT Id, TYPE__c FROM Stop_Positions__r) 
                FROM Load__c 
                where Load_Number__c in ({load_numbers_str})
                """
            self.result = self.sf_rest_session.query(query)
            return self.get_data_csv(self.result, 0)

        except Exception as e:
            logger.error(f"Error occurred during file download: {str(e)}")
            return None
        
    def making_driver_sql_request(self, load_numbers: List[str]) -> Optional[str]: # set_trucksql_request()
        """Downloads a CSV file with Trip pickup and delivery ID if it exists."""
        try:
            # Create the save folder if it doesn't exist
            os.makedirs(self.save_folder, exist_ok=True)
            
            # Ensure Salesforce session is initialized
            if not self.sf_rest_session:
                raise Exception('Salesforce REST session not initialized')

            # Prepare and execute Salesforce query
            load_numbers_str = ','.join([f"'{num}'" for num in load_numbers])
            query = f"""
                SELECT Id, DRIVER_ID__c, FirstName, LastName, 
                (SELECT Id, TYPE__c, END_DATE__c, UNIT__c FROM Vehicle_History__r WHERE END_DATE__c = null) 
                FROM Account WHERE RecordType.DeveloperName = 'DriverAccount'
                AND DRIVER_ID__c IN ({load_numbers_str})
                """
            self.result = self.sf_rest_session.query(query)
            return self.get_data_csv(self.result, 1)

        except Exception as e:
            logger.error(f"Error occurred during file download: {str(e)}")
            return None
    

    