from fastapi import FastAPI, File, UploadFile, HTTPException
from azure.storage.blob import BlobServiceClient
from typing import List
import io
import os
from loader import ablob, process_blob


app = FastAPI()

@app.get("/")
async def read_root():
    return {"message": "Hello, World!"}


# Azure Blob Storage credentials
azure_storage_connection_string = "DefaultEndpointsProtocol=https;AccountName=visualensemble;AccountKey=wto6fbBW7iH3WrKOcBVCAoWnCyTqWQc9A729jwcxGO72oxGvRbhiH9xiYj3+2+G9pXXmOtPEVZMF+AStLy/FeA==;EndpointSuffix=core.windows.net"
azure_container_name = "vision"

# Initialize BlobServiceClient
blob_service_client = BlobServiceClient.from_connection_string(azure_storage_connection_string)
container_client = blob_service_client.get_container_client(azure_container_name)

SUPPORTED_EXTENSIONS = {'.pdf', '.docx', '.xlsx', '.csv', '.txt', '.pptx'}




@app.post("/upload/")
async def upload_files(files: List[UploadFile] = File(...)):
    uploaded_urls = []
    #context =[]
    for file in files:
        # Ensure it's a file
        if not file.filename:
            raise HTTPException(status_code=400, detail="No file provided")

        # Check if file extension is supported
        ext = os.path.splitext(file.filename)[1].lower()
        if ext not in SUPPORTED_EXTENSIONS:
            raise HTTPException(status_code=400, detail=f"Unsupported document format: {ext}")

        # Read file content as bytes
        file_content = await file.read()

        # Upload file to Azure Blob Storage
        blob_client = container_client.get_blob_client(file.filename)
        with io.BytesIO(file_content) as stream:
            blob_client.upload_blob(stream)

        if ext == '.pdf':
            # Construct blob URL with SAS token
            blob_url = f"{blob_client.url}"
            con = process_blob(blob_url)
            #context.append(con)
            uploaded_urls.append(blob_url)
        else:
            # Pass the filename to a function for further processing
            #process_file(file.filename)
            con=ablob(azure_container_name,file.filename)
            #context.append(con)

        uploaded_urls.append(file.filename)

    for blob in container_client.list_blobs():
        container_client.delete_blob(blob)

    return {"vectors Performed and Uploaded"}