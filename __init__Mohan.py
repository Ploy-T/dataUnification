from distutils.command.upload import upload
from fileinput import filename
import os
import uuid
from aws_table_code import extract
import json
from azure.storage.blob import BlobClient,BlobServiceClient
import azure.functions as func
from azure.identity import DefaultAzureCredential
import logging
import traceback

import regex_extraction as re_extract

LOCAL_PATH='./uploads'


class AzureBlobPdfFile:
    def __init__(self,accountName,container_name,upload_container_name):
        accountUrl =  "https://"+accountName+".blob.core.windows.net"
        
        self.accountUrl = accountUrl
        self.accountUrl2 = accountUrl
        self.container_name=container_name
        self.upload_container_name = upload_container_name
        self.blob_service_client = BlobServiceClient(self.accountUrl,DefaultAzureCredential())
        #self.conn_str = conn_str
        self.blob_service_client1 = BlobServiceClient(self.accountUrl2,DefaultAzureCredential())
        


    
    def upload_pdf_file(self,upload_file_path,blob):
        logging.info("we are in upload_pdf_file")
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

def extractDate(raw_text):
    dateRegex = r"\nDATE:(.*)\n"
    date = re_extract.field_capture_with_regex(dateRegex, raw_text)

    return date

def make_folder():
    folder_name = os.path.join("process_files",uuid.uuid4().hex)
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)
    return folder_name

def create_folder(folder_path):
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    return True

def postprocess_aws_form(formResponse):
    formOutputs = {}
    for eachKey in formResponse.keys():
        keyValues = eachKey.split(":")
        if len(keyValues) > 1:
            keyPart = keyValues[0]
            valuePart = " ".join(keyValues[1:])
            formOutputs[keyPart] = valuePart + formResponse[eachKey]
        else:
            formOutputs[eachKey] = formResponse[eachKey]    
    return formOutputs


def filter_DSR_line_items(line_items):
    filtered_line_items = []
    for eachLineItem in line_items:
        if eachLineItem['description'].find('DSR') != -1:
            filtered_line_items.append(eachLineItem)

    return filtered_line_items

def extract_aws_form(fileName, blob):
    folder_name = make_folder()
    upload_path = os.path.join(folder_name, "uploads")
    
    create_folder(upload_path)
    #data_file.save(filename)
    upload_file_path = os.path.join(upload_path, fileName)
    with open(upload_file_path, "wb") as file:
                file.write(blob)


    imagePath = os.path.join(folder_name, "images")
    create_folder(imagePath)

    formResponse = extract.extract_forms(upload_file_path, imagePath)
    #print(textResponse)
    return postprocess_aws_form(formResponse)

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
        
            azure_blob_file = AzureBlobPdfFile(accountName,container_name,upload_container_path)
            blob = azure_blob_file.getBlobStream(fileName)

            output = extract_aws_form(fileName, blob)
            line_items = extract.extract_tables_from_text(output["raw_text"])

            filtered_line_items = filter_DSR_line_items(line_items)

            output["line_items"] = line_items
            output["extractedData"] = filtered_line_items
            output["DATE"] = extractDate(output["raw_text"])
            
            output["config"] = req_body

            # with open("/home/mohan/mohan/dsilo-ai/poc/bw_extraction/output.json", "w") as f:
            #     f.write(json.dumps(output))
            blob = fileName.replace('.pdf','')+'_'+'.json'
            with open(blob, 'w', encoding='utf-8') as f:
                json.dump(output, f, ensure_ascii=False, indent=4)
            azure_blob_file.upload_pdf_file(blob,blob)
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