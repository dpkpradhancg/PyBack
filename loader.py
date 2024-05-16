from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
import json
from langchain_community.document_loaders import PyPDFLoader
import io
from langchain_community.document_loaders import AzureBlobStorageFileLoader
from datetime import timedelta
import datetime
from langchain_openai import AzureOpenAIEmbeddings
from pymongo import MongoClient
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_community.vectorstores import MongoDBAtlasVectorSearch


embeddings = AzureOpenAIEmbeddings(
    azure_deployment="embedding",
    azure_endpoint="https://openai-poc01.openai.azure.com/",
    api_key="346bc051530f4de98af5414b1aef7124",
    model="text-embedding-3-large",
    disallowed_special=()
)

# initialize MongoDB python client
client = MongoClient('mongodb+srv://pd420786:etwRn7tzNxafhcBD@deepak.hukooer.mongodb.net/?retryWrites=true&w=majority&appName=Deepak')

DB_NAME = "Deepak"
COLLECTION_NAME = "pycollection"
ATLAS_VECTOR_SEARCH_INDEX_NAME = "vector_index"

MONGODB_COLLECTION = client[DB_NAME][COLLECTION_NAME]





def process_blob(blob_data):
    # Your processing logic here
    #print("Processing blob data:",blob_data )
    loader = PyPDFLoader(blob_data)
    pages = loader.load()
    text_splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=150)
    docs = text_splitter.split_documents(pages)
    print('PDFDATA: ', docs)
    vector_search = MongoDBAtlasVectorSearch.from_documents(
    documents=docs,
    embedding=embeddings,
    collection=MONGODB_COLLECTION,
    index_name=ATLAS_VECTOR_SEARCH_INDEX_NAME,
)
    print('Vector uploaded')

    #return pages

def download_blob(container_name, blob_name):
    connection_string = f"DefaultEndpointsProtocol=https;AccountName=pocopenaista01;AccountKey=6MHb9NYt77L8tNhkBc4ixYyzq3lL+pDg5J+agslpcyRZr38yxJMGu7mVA1n1Q+RHKuRp7M7/mn88+AStfvxT3A==;EndpointSuffix=core.windows.net"
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

    # Download blob data
    blob_data = blob_client.download_blob().readall()
    file_obj = io.BytesIO(blob_data)

    return file_obj
def ablob(container_name, blob_name):
    loader = AzureBlobStorageFileLoader(
    conn_str = f"DefaultEndpointsProtocol=https;AccountName=visualensemble;AccountKey=wto6fbBW7iH3WrKOcBVCAoWnCyTqWQc9A729jwcxGO72oxGvRbhiH9xiYj3+2+G9pXXmOtPEVZMF+AStLy/FeA==;EndpointSuffix=core.windows.net",
    container=container_name,
    blob_name=blob_name,
)
    pages = loader.load()
    text_splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=150)
    docs = text_splitter.split_documents(pages)
    print('BLOBDATA: ', docs)
    vector_search = MongoDBAtlasVectorSearch.from_documents(
    documents=docs,
    embedding=embeddings,
    collection=MONGODB_COLLECTION,
    index_name=ATLAS_VECTOR_SEARCH_INDEX_NAME,
)
    print('Vector uploaded')
    #return pages


def generate_sas_token(container_name, blob_name, account_key):
    blob_service_client = BlobServiceClient(account_url=f"https://pocopenaista01.blob.core.windows.net", credential=account_key)
    # Define the expiry time (1 minute from now)
    expiry_time = datetime.datetime.utcnow() + timedelta(minutes=1)
    
    # Generate SAS token for the blob
    sas_token = generate_blob_sas(
        blob_service_client.account_name,
        container_name,
        blob_name,
        account_key=account_key,
        permission=BlobSasPermissions(read=True),  # Specify the permissions
        expiry=expiry_time
    )
    
    return sas_token


def maincall():
    container_name = "construction"
    blob_name = "JourneyOnline.pdf"
    account_key = "6MHb9NYt77L8tNhkBc4ixYyzq3lL+pDg5J+agslpcyRZr38yxJMGu7mVA1n1Q+RHKuRp7M7/mn88+AStfvxT3A=="

    # sas_token = generate_sas_token(container_name, blob_name, account_key)
    
    # # Use the SAS token to access the blob
    # blob_url_with_sas = f"https://pocopenaista01.blob.core.windows.net/{container_name}/{blob_name}?{sas_token}"
    # print("Blob URL with SAS token:", blob_url_with_sas)

    #ablob(container_name,blob_name)
    #process_blob("https://visualensemble.blob.core.windows.net/vision/JourneyOnline.pdf")
 

maincall()