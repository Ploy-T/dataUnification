from fileinput import filename
import os
import uuid
import json
from azure.storage.blob import BlobClient,BlobServiceClient
import azure.functions as func
from azure.identity import DefaultAzureCredential
import logging
import traceback
import match

LOCAL_PATH='./uploads'


class AzureBlobFile:
    def __init__(self,accountName,container_name,upload_container_name):
        accountUrl =  "https://"+accountName+".blob.core.windows.net"
        
        self.accountUrl = accountUrl
        self.accountUrl2 = accountUrl
        self.container_name=container_name
        self.upload_container_name = upload_container_name
        self.blob_service_client = BlobServiceClient(self.accountUrl,DefaultAzureCredential())
        #self.conn_str = conn_str
        self.blob_service_client1 = BlobServiceClient(self.accountUrl2,DefaultAzureCredential())
    
    def upload_json(self,upload_file_path,blob):
        logging.info("we are in upload_json_file")
        blob_client = self.blob_service_client1.get_blob_client(container=self.upload_container_name.lower()+"/output",
                                                                blob=blob)
        with open(upload_file_path, "rb") as data:
            blob_client.upload_blob(data,overwrite=True)

    def getBlobStream(self, file_path):
        
        file_name = file_path.split("/")[-1]
        logging.info("file_name"+str(file_name))
        blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=file_name)
        isExist = blob_client.exists()
        logging.info(isExist)

        if isExist: 
            # download blob file
            stream = blob_client.download_blob()
            logging.info("File exists and is readable")
            bytes1 = stream.readall()
            
            return bytes1
        else:
            return ""
        return ""

def make_folder():
    folder_name = os.path.join("process_files",uuid.uuid4().hex)
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)
    return folder_name

def create_folder(folder_path):
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    return True

# parse form recognizer json object and concatenate first page lines
def extract_fr_json_text(lines):
    text = ""
    for line in lines:
        text = text + line["text"]
    return text
    
def upload_extract_vendor(fileName, masterFileName, blob):
    folder_name = make_folder()
    upload_path = os.path.join(folder_name, "uploads")
    
    create_folder(upload_path)
    #data_file.save(filename)
    upload_file_path = os.path.join(upload_path, fileName)
    with open(upload_file_path, "wb") as file:
        file.write(blob)

    f = open(upload_file_path)
    json_obj = json.loads(f)
    # print(json_obj)
    page_1_lines = json_obj["data"]["readResults"][0]["lines"]
    text = extract_fr_json_text(page_1_lines)
    ## assuming text = "vendorName"
    vendorName = {'verdorName': text}

    # upload master file in the same path as pdf file
    upload_master_path = os.path.join(upload_path, masterFileName)
    with open(upload_master_path, "wb") as file:
        file.write(blob)
    f = open(masterFileName)
    masterData = json.load(f)  


    
    currency = price_parsing.check_currency(text)
    if currency is None:
        currency = "USD"
    json_obj["currency"] = currency
    print("Currency :", currency)
    print(json_obj)
    return json_obj

#process only first 3 pages
#filePath = "/home/mohan/mohan/dsilo-ai/poc/bw_extraction/files/po/BAX326445.pdf"
#filePath = "/home/mohan/mohan/dsilo-ai/poc/bw_extraction/files/po/BAX332100.pdf"


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        req_body = req.get_json()
    except ValueError:
        pass

    if req_body:
        
        fileName = req_body["fileName"]
        container_name= req_body['containerName']
        upload_container_path = req_body['containerName']
        #fileName = file_path_json['fileName']
        accountName = req_body['accountName']
        try:
        
            azure_blob_file = AzureBlobFile(accountName,container_name,upload_container_path)
            blob = azure_blob_file.getBlobStream(fileName)

            output = upload_extract_currency(fileName,blob)
            
            output["config"] = req_body

            
            blob = fileName.replace('.pdf','')+'_'+'.json'
            with open(blob, 'w', encoding='utf-8') as f:
                json.dump(output, f, ensure_ascii=False, indent=4)
            azure_blob_file.upload_json(blob,blob)
            os.remove(blob)
            
            if output is not None:
                return json.dumps(output)
            else:
                return "No data extracted"

        except Exception as ex:
            print(f"Exception Sentence : {str(ex)}")
            logging.info(''.join(traceback.format_exception(etype=type(ex), value=ex, tb=ex.__traceback__)))
            response_msg = {'Error':str(''.join(traceback.format_exception(etype=type(ex), value=ex, tb=ex.__traceback__)))}
            response_msg = json.dumps(response_msg)

            return response_msg