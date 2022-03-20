

import sys
import os
import datetime
import logging
import subprocess
import csv
import shutil
import json
import pandas as pd
import copy
from config_averiasmasivas import *

#FUNCTIONS
def rm_hdfs(target_rm):
    target=target_rm
    
    cmd = '-rm'
    
    proc = subprocess.Popen(['hdfs', 'dfs', cmd,'-f', target],
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            universal_newlines=False)
    output, error = proc.communicate()
    if proc.returncode != 0:
        print_with_logging('No se logró eliminar el archivo del directorio SFTP '+target, 'error')
        print_with_logging(proc.returncode + ' '+ output+' ' + error, 'error')
    print_with_logging('Se elimino el archivo del directorio SFTP: '+target, 'info')
    return output

def hdfs_remove(fHDFSPath):
    path_file = fHDFSPath + '/*.csv'
    cmd = '-rm'
    proc = subprocess.Popen(['hdfs', 'dfs', cmd, '-f', path_file],
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            universal_newlines=False)
    output, error = proc.communicate()
    if proc.returncode != 0:
        print_with_logging('No se logró remover el archivo del directorio HDFS '+path_file, 'error')
        print_with_logging(proc.returncode + ' '+ output+' ' + error, 'error')
    print_with_logging('Se removió el archivo del directorio HDFS: '+path_file, 'info')
    return output

def sftp_to_hdfs(sftp, hdfs):
    in_path=sftp
    target=hdfs
    
    cmd = '-copyFromLocal'
    
    proc = subprocess.Popen(['hdfs', 'dfs', cmd, in_path, target],
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            universal_newlines=False)
    output, error = proc.communicate()
    if proc.returncode != 0:
        print_with_logging('No se logró mover el archivo del directorio SFTP '+in_path+' a HDFS: '+target, 'error')
        print_with_logging(proc.returncode + ' '+ output+' ' + error, 'error')
    print_with_logging('Se movió el archivo del directorio SFTP: '+in_path+' a HDFS: '+target, 'info')
    return output

def repeat_character(character, length):
    return character * length

def print_with_logging(str, level):
    print(str)
    if level == 'error':
        logging.error(str)
    elif level == 'warning':
        logging.warning(str)
    elif level == 'info':
        logging.info(str)
    else:
        logging.info(str)

#PARAMETROS
p_filename = sys.argv[1]
p_file_out = p_filename[:-5]#le quita la extension

#VARIABLES
path_log = V_PATH_LOG
path_file_input = V_PATH_FILE_AVERIAS_IN
path_file_csv_write = V_PATH_FILE_AVERIAS_OUT + '/' + p_file_out + '.csv'
path_hdfs_target = HDFS_PATH_INPUT_TICKETSMULTICONSULTA
path_file = path_file_input + '/' + p_filename

#path_file = '/srv/BigData/dev_var/Averias_Masivas/bin/averias_masivas.json'
#path_file = '/srv/BigData/dev_data/Averias_Masivas/input/averias_masivas.json'

#DIRECTORIO LOG
current_date_s = datetime.datetime.now()
path_day = current_date_s.strftime("%Y%m%d_%H%M%S")
print_with_logging("Se procesa la fecha: " + path_day, 'info')

# Creando directorio log para la fecha actual
current_date = datetime.datetime.now().strftime("%Y%m%d")
path_log_current_date = path_log + '/' + 'flatten_json' + '/' + current_date + '/'
if not os.path.exists(path_log_current_date):
    os.makedirs(path_log_current_date, 0o775)  # Crea la carpeta y asigna los permisos
    print_with_logging("Se crea el directorio: " + path_log_current_date, 'info')

process_time = datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")
file_log = path_log_current_date + 'script_averias_masivas_' + process_time + '.log'

#Se crea log de ejecución
#Resetear los handlers del logging
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)
logging.basicConfig(filename=file_log, filemode="w", level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

############################################# INICIO PROCESO #############################################

#Read Json
try:
    print_with_logging( repeat_character('-', 100), 'info' )
    print_with_logging( 'Lectura archivo JSON de EDGE', 'info' )
    print_with_logging( repeat_character('-', 100), 'info' )
    
    f = open(path_file,encoding="utf-8-sig")
    data = json.load(f)
    data_copy = copy.deepcopy(data) #Creamos copia profunda
except Exception as e:
    print_with_logging('No se logra cargar el archivo json: ' + path_file, 'error')
    print_with_logging('Detalle: ' + str(e), 'error')
    logging.error('Detalle: ', exc_info=e)
    sys.exit(1)

#Extraer campos
try:
    print_with_logging( repeat_character('-', 100), 'info' )
    print_with_logging( 'APLANAR JSON - EXTRAER CAMPOS', 'info' )
    print_with_logging( repeat_character('-', 100), 'info' )
    
    #constantes
    UNICA_Service=data['UNICA-ServiceId']
    UNICA_Aplication=data['UNICA-Application']
    UNICA_Pid=data['UNICA-PID']
    UNICA_User=data['UNICA-User']

    #eventos
    event_id=data['eventId']
    event_time=data['eventTime']
    event_type=data['eventType']
    event=data['event']

    #tickets
    ticket=event['ticket']
    Id=ticket['id']
    description=ticket['description']
    source=ticket['source']
    status=ticket['status']
    scheduledStart=ticket['scheduledStartDate']
    scheduledEnd=ticket['scheduledEndDate']
    start=ticket['startDate']
    resolution=ticket['resolutionDate']

    #relatedparty
    relatedParty = data["event"]["ticket"]["relatedParty"]

    #doc_final
    doc_final={}
    doc_final=[]

    relatedParty_for= [x for x in relatedParty]

    result=relatedParty_for

    for i in range(len(relatedParty)):
        id_item = data["event"]["ticket"]["relatedParty"][i]['id']
        data_copy["event"]["ticket"]["relatedParty"] = relatedParty[i]
        
        doc_final.append(
        {
            "unica_serviceid": UNICA_Service,
            "unica_application": UNICA_Aplication,
            "unica_pid":UNICA_Pid,
            "unica_user":UNICA_User,
            "eventid":event_id,
            "eventtime":event_time,
            "eventtype":event_type,
            "event_ticket_id": Id,
            "event_ticket_source": source,
            "event_resolutionDate":resolution,
            "event_ticket_releatedparty_id":data_copy["event"]["ticket"]["relatedParty"]["id"],
            "event_ticket_releatedparty_role": data_copy["event"]["ticket"]["relatedParty"]["role"],
            "event_ticket_releatedparty_legalid_nationalidtype":data_copy["event"]["ticket"]["relatedParty"]["legalId"][0]["nationalIdType"],
            "event_ticket_releatedparty_legalid_nationalid":data_copy["event"]["ticket"]["relatedParty"]["legalId"][0]["nationalId"],
            "event_ticket_product_publicid":data_copy["event"]["ticket"]["product"]["publicId"],
            "event_ticket_product_status":data_copy["event"]["ticket"]["product"]["status"],
            "event_ticket_product_additionaldata_key":data_copy["event"]["ticket"]["product"]["additionalData"][0]["key"],
            "event_ticket_product_additionaldata_value":data_copy["event"]["ticket"]["product"]["additionalData"][0]["value"]
        }
        )
    
except Exception as e:
    print_with_logging('Ocurrieron errores al extraer campos: ', 'error')
    print_with_logging('Detalle: ' + str(e), 'error')
    logging.error('Detalle: ', exc_info=e)
    sys.exit(1)

try:
    print_with_logging( repeat_character('-', 100), 'info' )
    print_with_logging( 'Escribir archivo CSV en EDGE', 'info' )
    print_with_logging( repeat_character('-', 100), 'info' )
    
    with open(path_file_csv_write, "w", encoding='utf-8', newline="") as csv_file:
        cols = ["unica_serviceid","unica_application","unica_pid","unica_user","eventid","eventtime",
                "eventtype","event_ticket_id","event_ticket_source","event_resolutionDate","event_ticket_releatedparty_id","event_ticket_releatedparty_role",
               "event_ticket_releatedparty_legalid_nationalidtype","event_ticket_releatedparty_legalid_nationalid","event_ticket_product_publicid",
               "event_ticket_product_status","event_ticket_product_additionaldata_key","event_ticket_product_additionaldata_value"] 
        writer = csv.DictWriter(csv_file, fieldnames=cols,delimiter='|')
        writer.writerows(doc_final)
    
    #df_averias = pd.read_csv(path_file_csv_write)
    #print_with_logging('El CSV generado tiene la siguiente información: '+df_averias.info(),'info')
    
except Exception as e:
    print_with_logging('No se logra escribir el archivo CSV en EDGE: ' + path_file_csv_write, 'error')
    print_with_logging('Detalle: ' + str(e), 'error')
    logging.error('Detalle: ', exc_info=e)
    sys.exit(1)

#df_averias = pd.read_csv('/srv/BigData/dev_var/Averias_Masivas/bin/data_2.csv')
#print(df_averias.info())

try:
    print_with_logging( repeat_character('-', 100), 'info' )
    print_with_logging( 'Copia de archivos en HDFS', 'info' )
    print_with_logging( repeat_character('-', 100), 'info' )
    
    #hdfs_remove(path_hdfs_target)
    #print_with_logging('Se borraron todos los archivos previos en HDFS', 'info')
    
    sftp_to_hdfs(path_file_csv_write, path_hdfs_target)
    print_with_logging('Se copiaron los archivos CSV a HDFS', 'info')
    
except Exception as e:
    print_with_logging('Ocurrió un error en el borrado previo y carga de archivos en HDFS ', 'error')
    print_with_logging('Detalle: ' + str(e), 'error')
    logging.error('Detalle: ', exc_info=e)
    sys.exit(1)

#TARGET="/TEF/dev/Averias_Masivas/tickets_multiconsulta/input"
#hdfs_remove(path_hdfs_target)
    
#PATH_SFTP="/srv/BigData/dev_var/Averias_Masivas/bin/data_2.csv"
#PATH_HDFS="/TEF/dev/Averias_Masivas/tickets_multiconsulta/input"
#sftp_to_hdfs(PATH_SFTP,PATH_HDFS)

print_with_logging(repeat_character('-', 100), 'info')
print_with_logging('Proceso Finalizado.', 'info')
print_with_logging(repeat_character('-', 100), 'info')

   
    
    
    
    
    
    
    
