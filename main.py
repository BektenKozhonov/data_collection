import logging
import os
import requests
import pandas as pd
from utils.salesforce_interfrnc import SalesforceAuthentication
from io import BytesIO

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Define constants
UPLOAD_FOLDER = 'temp/'

def ensure_folder_exists(folder_path):
    """Ensures the given folder exists."""
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

def download_and_save_file(content_document_id: str, save_folder: str):
    """
    Downloads the latest ContentVersion file associated with the given ContentDocumentId from Salesforce
    and saves it in the specified folder.
    """
    try:
        auth = SalesforceAuthentication()
        sf_rest_session, _ = auth.get_sessions()

        if not sf_rest_session:
            raise Exception('Salesforce REST session not initialized')

        # Query for the latest ContentVersion
        query = f"""
        SELECT Id, Title, VersionData, FileExtension
        FROM ContentVersion
        WHERE ContentDocumentId = '{content_document_id}'
        ORDER BY LastModifiedDate DESC
        LIMIT 1
        """
        result = sf_rest_session.query(query)

        if not result['records']:
            raise Exception(f"No ContentVersion found for ContentDocumentId {content_document_id}")

        content_version = result['records'][0]
        content_version_id = content_version['Id']
        file_extension = content_version['FileExtension']

        # Download file
        download_url = f"https://{sf_rest_session.sf_instance}/services/data/v61.0/sobjects/ContentVersion/{content_version_id}/VersionData"
        headers = {'Authorization': f'Bearer {sf_rest_session.session_id}'}
        response = requests.get(download_url, headers=headers)

        if response.status_code != 200:
            raise Exception(f"Error downloading file: {response.content}")

        logger.info(f"File successfully downloaded to memory. File extension: {file_extension}")
        return BytesIO(response.content), file_extension

    except Exception as e:
        logger.error(f"Error during file download for ID {content_document_id}: {e}")
        return None

def pdf_to_text(file_path):
    images = convert_from_path(file_path)
    text = ""
    for image in images:
        text += pytesseract.image_to_string(image)

    return text

def extract_text_from_pdf(pdf_path):
    with open(pdf_path, 'rb') as file:
        # Initialize PDF reader
        pdf_reader = PyPDF2.PdfReader(file)
        text = ''
        # Iterate through each page and extract text
        for page_num in range(len(pdf_reader.pages)):
            page = pdf_reader.pages[page_num]
            text += page.extract_text()
    if len(text) < 1000:
      text = pdf_to_text(pdf_path)
      if len(text) < 1000:
        raise ValueError('Your file might be empty')

    return text

def receive_file():
    """
    Main function to process files from a given CSV input.
    """
    try:
        auth = SalesforceAuthentication()
        sf_rest_session, _ = auth.get_sessions()

        if not sf_rest_session:
            logger.error("Failed to initialize Salesforce session.")
            return

        # Read and process the CSV file
        try:
            df = pd.read_csv('./set/output.csv')
            if 'DocumentId' not in df.columns:
                raise KeyError("Missing 'DocumentId' column in CSV file.")
        except Exception as e:
            logger.error(f"Error reading CSV file: {e}")
            return

        for doc_id in df['DocumentId']:
            download_and_save_file(doc_id, UPLOAD_FOLDER)

    except Exception as e:
        logger.error(f"Error in receive_file: {e}")

if __name__ == '__main__':
    receive_file()
