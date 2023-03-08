from distutils.command.upload import upload
from fileinput import filename
import os
import uuid
# from aws_table_code import extract
import json
from azure.storage.blob import BlobClient,BlobServiceClient
import azure.functions as func
from azure.identity import DefaultAzureCredential
import logging
import traceback
from aws_table_code import extract
from src_code.extractinfopipeline import ExtractInfoPipeline
import regex_extraction as re_extract
import nltk
nltk.download('wordnet')
nltk.download('stopwords')
nltk.download('punkt')
nltk.download('averaged_perceptron_tagger')
nltk.download('maxent_ne_chunker')
nltk.download('words')
nltk.download('omw-1.4')

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

def extract_aws_form(fileName, blob, keywords):
    folder_name = make_folder()
    upload_path = os.path.join(folder_name, "uploads")

    create_folder(upload_path)
    #data_file.save(filename)
    upload_file_path = os.path.join(upload_path, fileName)
    with open(upload_file_path, "wb") as file:
                file.write(blob)

    
#     imagePath = os.path.join(folder_name, "images")
#         create_folder(imagePath)
    
#     formResponse = extract.extract_forms(upload_file_path, imagePath)
#     postprocee_requst = postprocess_aws_form(formResponse)
    
    
#     upload_file_path_json = os.path.join(upload_path, fileName+"_output.json")
#     with open(upload_file_path_json, "wb") as file:
#                 file.write(blob)
            
    logging.info("Entered extraction 1")
    entities_obj = ExtractInfoPipeline(upload_file_path, upload_file_path, keywords)
    logging.info("Entered extraction 2")
    entities_json = entities_obj.extract()
#     logging.info("Entered extraction 3")
#             response_msg = entities_json
    #print(textResponse)
    return entities_json #postprocess_aws_form(entities_json)

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
        blobpath = req_body["path"]
        innerPath = req_body["path"].split('/')
        keywords = json.loads(req_body["keywords"])
        if(len(innerPath) > 1):
            container_name= req_body['containerName'] + '/' + innerPath[0]
        else :
            container_name= req_body['containerName']
        upload_container_path = req_body['containerName']
        #fileName = file_path_json['fileName']
        accountName = req_body['accountName']
        try:
            print (container_name)
            print (blobpath)
            logging.info (container_name)
            logging.info (blobpath)
            azure_blob_file = AzureBlobPdfFile(accountName,container_name,upload_container_path)
            blob = azure_blob_file.getBlobStream(fileName)
            
            output = extract_aws_form(fileName, blob, keywords)
            
            
            if output == None:
                output = {}
            print(output)
            output["config"] = req_body
            
#             line_items = extract.extract_tables_from_text(output["raw_text"])

#             filtered_line_items = filter_DSR_line_items(line_items)

#             output["line_items"] = line_items
#             output["extractedData"] = filtered_line_items
#             output["DATE"] = extractDate(output["raw_text"])

#             output["config"] = req_body

            # with open("/home/mohan/mohan/dsilo-ai/poc/bw_extraction/output.json", "w") as f:
            #     f.write(json.dumps(output))
            blob = fileName.replace('.json','')+'_output'+'.json'
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
