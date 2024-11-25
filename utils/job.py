import logging
import pandas as pd
import numpy as np
import os
from utils.salesforce_interfrnc import SalesforceAuthentication, BulkLoadProcessor, TripSetter
import re

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class DataSet:
    def __init__(self, filepath):
        self.df = pd.read_excel(filepath)
        self.process_df()
    
    def process_df(self):
        self.df.columns = [
            'company_load_number',
            'contract_spot',
            'sales_rep',
            'customer',
            'position',
            'status',
            'number_of_picks',
            'pu_info',
            'pu_state_code',
            'pu_time',
            'driver_pu_time',
            'number_of_drops',
            'del_info',
            'del_state_code',
            'del_time',
            'driver_del_time',
            'driver',
            'linehaul',
            'fuel_surcharge',
            'lumper',
            'linehaul_total',
            'empty_miles',
            'loaded_miles',
            'dollar_per_mile_loaded',
            'dollar_per_mile_total',
            'actions'
        ]
        
        self.df = self.df.loc[2001:2100, [
            'customer',
            'status',
            'pu_info',
            'pu_state_code',
            'pu_time',
            'del_info',
            'del_state_code',
            'del_time',
            'driver',
            'linehaul_total',
            'lumper',
            'empty_miles',
            'loaded_miles'
        ]]
        
        self.df['load'] = self.df.customer.map(lambda i: i.split(' ')[-1])
        self.df['customer'] = self.df.customer.apply(lambda i: " ".join(i.split(' ')[:-1]))
        self.df['pu_city'] = self.df.pu_info.apply(lambda i: i.split(', ')[0])
        self.df['del_city'] = self.df.del_info.apply(lambda i: i.split(', ')[0])
        self.df['driver_id'] = self.df.driver.apply(lambda i: i.split(' - ')[0] if pd.notna(i) else '')
        self.df['driver'] = self.df.driver.apply(lambda i: i.split(' - ')[1].replace(' (100.0%)', '') if pd.notna(i) else '')
        self.df['driver'] = self.df.driver.fillna('')
        self.df['driver_id'] = self.df.driver_id.fillna('')


class TripDataset(DataSet, TripSetter):
    def __init__(self, filepath, savepath):
        DataSet.__init__(self, filepath)
        TripSetter.__init__(self, savepath)
        del_pick_path = self.making_trip_sql_request(self.df.load)
        trip_key_path = self.making_driver_sql_request(self.df.driver_id)
        self.csv_data = pd.read_csv(del_pick_path)
        self.trip_data = pd.read_csv(trip_key_path)
        self.process_csv_data()
        self.process_trip_data()
        self.data_merge()
    
    def extract_pickup_and_delivery_ids(self, data):
        """
        Извлекает идентификаторы Pickup и Delivery из строки данных.

        :param data: строка, содержащая данные в формате OrderedDict.
        :return: словарь с pickup_ids и delivery_ids.
        """
        # Регулярное выражение для поиска Pickup и Delivery с их ID
        matches = re.findall(r"'TYPE__c': 'Pickup'.*?'Id': '(\w+)'|"
                            r"'TYPE__c': 'Delivery'.*?'Id': '(\w+)'", data)

        # Формирование словаря с результатами
        self.result = {"pickup_ids": [], "delivery_ids": []}
        for pickup_id, delivery_id in matches:
            if pickup_id:
                self.result["pickup_ids"].append(pickup_id)
            if delivery_id:
                self.result["delivery_ids"].append(delivery_id)

        return self.result
    
    def extract_vehicle_data(self, data):
        """
        Extracts 'Id', 'TYPE__c', 'UNIT__c' from OrderedDict data for vehicles.

        :param data: A string representation of OrderedDict data.
        :return: A dictionary with extracted fields.
        """
        # Regular expressions to match required fields
        vehicle_id = re.search(r"'Id': '(\w+)'", data)
        vehicle_type = re.search(r"'TYPE__c': '(\w+)'", data)
        unit_id = re.search(r"'UNIT__c': '(\w+)'", data)

        # Extract values if found
        return {
            'vehicle_id': vehicle_id.group(1) if vehicle_id else None,
            'vehicle_type': vehicle_type.group(1) if vehicle_type else None,
            'unit_id': unit_id.group(1) if unit_id else None,
        }
    
    def process_csv_data(self):
        try:
            self.csv_data.Stop_Positions__r = self.csv_data.Stop_Positions__r.map(lambda i: self.extract_pickup_and_delivery_ids(i))
            self.csv_data['pickup_id'] = self.csv_data.Stop_Positions__r.apply(lambda x: x['pickup_ids'][0] if x['pickup_ids'] else None)
            self.csv_data['delivery_id'] = self.csv_data.Stop_Positions__r.apply(lambda x: x['delivery_ids'][0] if x['delivery_ids'] else None)
            self.csv_data = self.csv_data.loc[:,['Load_Number__c', 'pickup_id', 'delivery_id']]
            self.csv_data.columns = ['load', 'pickup_id', 'delivery_id']
        except Exception as e:
            logger.error(f'ERROR: YOUR DATA MIGHT NOT HAVE PICKUP OR DELIVERY, OR {e}')

    def process_trip_data(self):
        try:
            self.trip_data.Vehicle_History__r = self.trip_data.Vehicle_History__r.map(lambda i: self.extract_vehicle_data(i) if pd.notna(i) else i)
            self.trip_data['vehicle_type'] = self.trip_data.Vehicle_History__r.map(lambda i: i['vehicle_type'] if pd.notna(i) else i)
            self.trip_data['unit_id'] = self.trip_data.Vehicle_History__r.map(
                lambda i: i['unit_id'] if isinstance(i, dict) and i.get('vehicle_type') == 'TRAILER' else np.nan
            )
            self.trip_data['vehicle_id'] = self.trip_data.Vehicle_History__r.map(
                lambda i: i['vehicle_id'] if isinstance(i, dict) and i.get('vehicle_type') == 'TRUCK' else np.nan
            )
            self.trip_data = self.trip_data.loc[:, ['DRIVER_ID__c', 'vehicle_type', 'unit_id', 'vehicle_id']]
            self.trip_data.columns = ['driver_id', 'vehicle_type', 'unit_id', 'vehicle_id']
        except Exception as e:
            logger.error(f'PROCESS TRIP DATA ERROR: {e}')
    
    def data_merge(self):
        try:
            self.df = pd.merge(self.df, self.csv_data, on='load', how='inner')

            # Convert 'driver_id' in both DataFrames to the same type
            self.df['driver_id'] = self.df['driver_id'].astype(str)
            self.trip_data['driver_id'] = self.trip_data['driver_id'].astype(str)

            # Merge after conversion
            self.df = pd.merge(self.df, self.trip_data, on='driver_id', how='inner')
        except Exception as e:
            logger.error(f'MERGING DATA ERROR: {e}')

        



class LoadRecord(DataSet, BulkLoadProcessor, SalesforceAuthentication):
    
    def __init__(self, file_path: str):
        # Инициализация всех родительских классов
        DataSet.__init__(self, file_path)
        BulkLoadProcessor.__init__(self)
        SalesforceAuthentication.__init__(self)


    def process_load_records(self):
        """Обрабатывает строки DataFrame и добавляет их в bulk загрузку."""
        for index, row in self.df.iterrows():
            try:
                load_data = {
                                'Name': row['load'],
                                'Load_Number__c': row['load'],
                                'LINEHAUL_RATE__c': float(row['linehaul_total']),
                                'EQUIPMENT_TYPE__c': 'DRY VAN',
                                'NOTES__c': row['driver'],
                                'STATUS__c': row['status'],
                                'IsHistory__c': 'true'
                            }
                self.add_load(load_data)
            except Exception as e:
                logger.error(f'Error processing load record at index {index}: {e}')

        # Отправляем bulk данные
        self.send_bulk_data('Load__c')
    
    

    def process_file(self):
        """Чтение и обработка загруженного файла CSV."""
        self.process_load_records()


class PickupDelivery(DataSet, BulkLoadProcessor, SalesforceAuthentication):
    
    def __init__(self, file_path: str):
        # Инициализация всех родительских классов
        DataSet.__init__(self, file_path)
        BulkLoadProcessor.__init__(self)
        SalesforceAuthentication.__init__(self)

        
    def parse_date(self, pu_time: str) -> dict:
        if not pu_time or not isinstance(pu_time, str):
            return None

        match = re.match(
            r'(?P<date>(?P<month>\d{2})/(?P<day>\d{2})/(?P<year>\d{4}))\s+'
            r'(?P<start_time>\d+:\d+)\s*-\s*(?P<end_time>\d+:\d+)(?P<timezone>[A-Z]+)',
            pu_time
        )
        if not match:
            logger.error(f"Time format is incorrect: {pu_time}")
            return None
        return match.groupdict()

    def appointment_date(self, pu_time: str) -> list:

        parsed = self.parse_date(pu_time)
        if not parsed:
            return [None, None]

        start_datetime = f"{parsed['year']}-{parsed['month']}-{parsed['day']}T{parsed['start_time']}:00"
        end_datetime = f"{parsed['year']}-{parsed['month']}-{parsed['day']}T{parsed['end_time']}:00"
        return [start_datetime, end_datetime]
    
    def picup_dlvr_loader(self):
        
        for index, row in self.df.iterrows():
            try:
                
                 # Create load data for stop 1 (Pickup)
                load_data_1 = {
                    'LOAD__r.Load_Number__c': row['load'],
                    'Name': row['pu_info'],
                    'TYPE__c': 'Pickup',
                    'APPOITMENT_START__c': self.appointment_date(row['pu_time'])[0],
                    'APPOITMENT_END__c': self.appointment_date(row['pu_time'])[1],
                    'LOCATION__City__s': row['pu_city'],
                    'LOCATION__CountryCode__s': 'US',
                    'LOCATION__PostalCode__s': 'zip',
                    'LOCATION__StateCode__s': row['pu_state_code'],
                    'LOCATION__Street__s': 'st'
                }

                load_data_2 = {
                    'LOAD__r.Load_Number__c': row['load'],
                    'Name': row['del_info'],
                    'TYPE__c': 'Delivery',
                    'APPOITMENT_START__c': self.appointment_date(row['pu_time'])[0],
                    'APPOITMENT_END__c': self.appointment_date(row['pu_time'])[0],
                    'LOCATION__City__s':row['del_city'],
                    'LOCATION__CountryCode__s':'US',
                    'LOCATION__PostalCode__s': 'zip',
                    'LOCATION__StateCode__s': row['del_state_code'],
                    'LOCATION__Street__s': 'st'
                }

                # Append both load data dictionaries to the result list
                self.add_load(load_data_1)
                self.add_load(load_data_2)

            except Exception as e:
                        logger.error(f'Error processing load record at index {index}: {e}')
        
        self.send_bulk_data('Stop_Position__c')

    def process_file(self):
        """Чтение и обработка загруженного файла CSV."""
        self.picup_dlvr_loader()

class Trip(TripDataset, BulkLoadProcessor):
    def __init__(self, file_folder: str, save_folder: str):
        TripDataset.__init__(self, file_folder, save_folder)

    def process_trip_records(self):
        """Обрабатывает строки DataFrame и добавляет их в bulk загрузку."""
        for index, row in self.df.iterrows():
            try:
                load_data = {
                                'AccountId__r.DRIVER_ID__c': row['driver_id'],
                                'LOAD__r.LOAD_NUMBER__c': row['load'],
                                'DEL__c': row['delivery_id'],
                                'DRIVER_PAY__c': float(row['linehaul_total']),
                                'DV__c': row['vehicle_id'], # поменяем sql из данных будем брат
                                'EMPTY_MI__c': row['empty_miles'],
                                'LOADED_MI__c': row['loaded_miles'],
                                'PICK__c': row['pickup_id'],
                                'PICKUP__c': row['pu_info'], 
                                'DELIVERY__c': row['del_info'],
                                'TRAILER__c': row['unit_id'], # поменяем sql из данных будем брать
                                'TRIP_STATUS__c': row['status']
                            }
                
                self.add_load(load_data)
            except Exception as e:
                logger.error(f'Error processing load record at index {index}: {e}')

        # Отправляем bulk данные
        self.send_bulk_data('Trip__c')

    def process_file(self):
        """Чтение и обработка загруженного файла CSV."""

        try:
            self.process_trip_records()

        except Exception as e:
            logger.error(f"Error processing file {self.file_path}: {e}")