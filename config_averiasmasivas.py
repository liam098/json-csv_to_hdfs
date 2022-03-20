## VARIABLES GLOBALES AVERIASMASIVAS
V_PATH_LOG = '/srv/BigData/dev_var/Averias_Masivas/log'

##VARIABLES INPUT
V_PATH_FILE_AVERIAS_IN = '/srv/BigData/dev_data/Averias_Masivas/input'

##VARIABLES OUTPUT
V_PATH_FILE_AVERIAS_OUT = '/srv/BigData/dev_data/Averias_Masivas/output'

HDFS_PATH_INPUT_MAESTRACONTACTOS = '/TEF/dev/Averias_Masivas/maestra_contactos/input'
HDFS_PATH_INPUT_MAESTRACONTACTOS_PROCESADOS = '/TEF/dev/Averias_Masivas/maestra_contactos/processed'
COLUMNS_MAESTRACONTACTOS = [
'nationalidtype',
'nationalid',
'number',
'name']

HDFS_PATH_INPUT_TICKETSMULTICONSULTA = '/TEF/dev/Averias_Masivas/tickets_multiconsulta/input'
HDFS_PATH_INPUT_TICKETSMULTICONSULTA_PROCESADOS = '/TEF/dev/Averias_Masivas/tickets_multiconsulta/processed'
COLUMNS_TICKETSMULTICONSULTA = [
'unica_serviceid',
'unica_application',
'unica_pid',
'unica_user',
'eventid',
'eventtime',
'eventtype',
'event_ticket_id',
'event_ticket_source',
'event_ticket_resolutiondate',
'event_ticket_releatedparty_id',
'event_ticket_releatedparty_role',
'event_ticket_releatedparty_legalid_nationalidtype',
'event_ticket_releatedparty_legalid_nationalid',
'event_ticket_product_publicid',
'event_ticket_product_status',
'event_ticket_product_additionaldata_key',
'event_ticket_product_additionaldata_value'
]                                

MULTITRAZA_FILE_HDFS = '/TEF/dev/Averias_Masivas/multiconsulta_trazabilidad/tmp'


#HDFS_PATH_CONSOLIDATED_CUSTOMER = '/TEF/dev/CampaniaPushRTD_2603/movil/consolidated/customer'                                
#HDFS_PATH_RENAMED_CUSTOMER = '/TEF/dev/CampaniaPushRTD_2603/movil/consolidated/customer/renamed'

#COLUMNS_TABLE = 'id STRING, name STRING, price_type STRING, recurring_charge_period STRING, price_amount STRING, price_unit STRING, assigned_product_id STRING, plan_indicator_flag STRING, movistar_total_flag STRING, assigned_maincomponent_key STRING, id_billing_offer STRING, ETL_UPDT STRING, fecha_carga STRING'
#PERM_FILE_HDFS = '/TEF/dev/aldm/BillingOffer/tmp'

#PERM_TABLE_HDFS = '/TEF/dev/hive_db/dev_perm.db/billingoffer_fija'
#MULTITRAZA_TABLE_HDFS = '/TEF/dev/hive_db/dev_perm.db/billingoffer_fija'

#HDFS_PATH_INPUT_BILLINGOFFER_PROCESADOS = '/TEF/dev/aldm/BillingOffer/processed'


