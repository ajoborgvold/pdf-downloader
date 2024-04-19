import os
import PyPDF2.errors
import requests
import openpyxl
import PyPDF2
import asyncio
import aiohttp
import pandas as pd

def get_pdf_folder():
    '''
    Get the path for the folder, "rapporter", storing the downloaded PDF files.
    If the folder does not exist, then create it.
    '''
    
    current_dir = os.getcwd()    
    reports_folder_path = os.path.join(current_dir, "rapporter")
    os.makedirs(reports_folder_path, exist_ok=True)
    
    return reports_folder_path

def create_new_data_file(file_path):
    '''
    Create a new Excel file with two columns and the headings "BRnum" and "Downloadstatus".
    This file will contain a list of all BRnums and their corresponding download status.
    '''
    
    workbook = openpyxl.Workbook()
    sheet = workbook.active
    
    headings = ["BRnum", "Downloadstatus"]
    sheet.append(headings)
    workbook.save(file_path)
    
    return workbook, sheet

def get_report_workbook():
    '''
    Get the Excel file, "rapport_oversigt_2017_2020", for storing data about downloaded PDF files.
    If the file does not exist, then create it.
    '''
    
    report_data_file_path = "rapport_oversigt_2017_2020.xlsx"
    workbook_exists = os.path.isfile(report_data_file_path)
    existing_BR_nums = None
    
    if workbook_exists:
        workbook = openpyxl.load_workbook(report_data_file_path)
        sheet = workbook.active
        
        existing_BR_nums = [cell.value for cell in sheet['A'][1:]]
    else:
        workbook, sheet = create_new_data_file(report_data_file_path)
    
    return report_data_file_path, workbook_exists, workbook, sheet, existing_BR_nums

def add_new_data(file_path, workbook, sheet, BR_num, status):
    '''
    Add new data to the existing Excel file, "rapport_oversigt_2017_2020".
    For each row in the original data file, the function will add BRnum and download status to the file.
    '''
    
    data = [BR_num, status]
    sheet.append(data)
    workbook.save(file_path)

def verify_pdf_integrity(file_path, response):
    '''
    Verify the integrity of downloaded files.
    Check if the file size is greater than 0.
    Check if the expected size and the actual size are the same.
    Try opening and reading the file. If the number of pages is greater than 0, the file is considered to be a valid and readable PDF. Otherwise the file is considered corrupt.
    '''
    
    try:
        expected_size = int(response.headers.get('Content-Length', 0))
        actual_size = len(response.content)
        if actual_size == 0 or expected_size != actual_size:
            return False
        else:
            with open(file_path, "rb") as file:
                reader = PyPDF2.PdfReader(file)
                
                if len(reader.pages) > 0:
                    return True
                else:
                    return False
    except PyPDF2.errors.PyPdfError:
        return False

def handle_downloaded_file(response, old_file_path, BR_num, folder_path):
    '''
    Call the veriry_pdf_integrity function and store the returned boolean in the variable is_pdf_ok.
    If the PDF-file is considered ok, rename it to the BR_num and store it in the folder "rapporter". If not, remove the file.
    Return the download status, either "downloadet" or "ikke downloadet".
    '''
    
    is_pdf_ok = verify_pdf_integrity(old_file_path, response)
    
    if is_pdf_ok:
        new_file_name = f"{BR_num}.pdf"
        new_file_path = os.path.join(folder_path, new_file_name)
        os.rename(old_file_path, new_file_path)
        return "downloadet"
    else:
        os.remove(old_file_path)
        return "ikke downloadet"

def download_new_file(response, folder_path, BR_num, url):
    '''
    Download PDF files.
    Rename the files with the BR_num.
    Verify the integrity of the downloaded files. If a file is corrupt, then delete it from the folder.
    '''
        
    status = None
    old_file_name = os.path.basename(url)
    old_file_path = os.path.join(folder_path, old_file_name)
    
    try:
        with open(old_file_path, "wb") as pdf_object:
            pdf_object.write(response.content)
        
        status = handle_downloaded_file(response, old_file_path, BR_num, folder_path)
        
    except IsADirectoryError:
        pdf_files = []
        for file_entry in os.scandir(old_file_path):
            if file_entry.is_file() and file_entry.name.endswith('.pdf'):
                pdf_files.append(file_entry.path)
        
        if pdf_files:
            first_pdf_file = pdf_files[0]
            with open(first_pdf_file, "wb") as pdf_object:
                pdf_object.write(response.content)
            
            status = handle_downloaded_file(response, first_pdf_file, BR_num, folder_path)
            
        else:
            status = "ikke downloadet"
    except:
        status = "ikke downloadet"
    
    return status

def request_connection(url):
    '''
    Make a request to the url received as an argument. Return the response.
    '''
    
    query_parameters = {"downloadformat": "pdf"}
    connect_timeout = 10
    read_timeout = 30
    
    response = requests.get(url, timeout=(connect_timeout, read_timeout), stream=True, params=query_parameters)
    return response

def main(incoming_data_file_path):
    '''
    Read the incoming data file row by row.
    Get the Excel file for storing data about download status for each row.
    Get the folder for storing the downloaded PDF files.
    
    Check if the row has already been processed. If so, skip it and move on to the next.
    Check if there is a URL in at least one of the two columns containing URLs for the PDF files. If not, set download_status to "manglende filsti".
    
    Make a request to the first URL, "Pdf_URL". If the response is ok, call the download_new_file function and store the returned download_status in the download_status variable.
    If download_status is "ikke downloadet", make a request to the second URL, "Report Html Address". A "response.ok == True" is handled in the same way as for "Pdf_URL". If the response is not ok, set the download_status to "ikke downloadet".
    If the response from the first request to "Pdf_URL" is not ok, move on to the "except" block and make a request to "Html Report Address". There code here runs in the same way as described above.
    
    Finally, if download_status is not "None", i.e. the row has been processed, call the add_new_data function.
    '''
        
    df = pd.read_excel(incoming_data_file_path)
    
    report_data_file_path, workbook_exists, workbook, sheet, existing_BR_nums = get_report_workbook()
    reports_folder_path = get_pdf_folder()

    for _, row in df[:1000].iterrows():
        if workbook_exists and row['BRnum'] in existing_BR_nums:
            continue
        
        res = None
        download_status = None
                
        if pd.isna(row["Pdf_URL"]) and pd.isna(row["Report Html Address"]):
            download_status = "manglende filsti"
        else:
            try:
                res = request_connection(row["Pdf_URL"])
                if res.ok:
                    download_status = download_new_file(res, reports_folder_path, row["BRnum"], row["Pdf_URL"])

                    if download_status == "ikke downloadet":
                        try:
                            res = request_connection(row["Report Html Address"])
                            
                            if res.ok:
                                download_status = download_new_file(res, reports_folder_path, row["BRnum"], row["Report Html Address"])
                            else:
                                download_status = "ikke downloadet"
                                
                        except requests.exceptions.RequestException:
                            download_status = "ikke downloadet"

                        except:
                            download_status = "ikke downloadet"
                            
                else:
                    download_status = "ikke downloadet"
                    
            except requests.exceptions.RequestException:
                try:
                    res = request_connection(row["Report Html Address"])
                    
                    if res.ok:
                        download_status = download_new_file(res, reports_folder_path, row["BRnum"], row["Report Html Address"])
                    else:
                        download_status = "ikke downloadet"
                        
                except requests.exceptions.RequestException:
                    download_status = "ikke downloadet"
                    
                except:
                    download_status = "ikke downloadet"
                    
            except:
                download_status = "ikke downloadet"

        if download_status is not None:
            add_new_data(report_data_file_path, workbook, sheet, row['BRnum'], download_status)


'''
Import the data (Excel file) from which you want to download PDF files into the project directory.
Pass the file path as an argument to the function below, "read_data()".
'''
main("GRI_2017_2020.xlsx")