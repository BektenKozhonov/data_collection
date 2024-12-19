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



    