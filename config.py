PROJECT_ID = 'datawarehouse-datametica-poc'
CG_BUCKET_NAME = 'customer_grouping'
CP_BUCKET_NAME = 'azure_dataverse' 
STG_DATASET_ID = 'stg'
CP_CSV_PATH = 'gs://azure_dataverse/customer_product_processing/'
CP_CSV_PATH_REGEX = 'customer_product_processing/'
CP_TEMP_TABLE = 'customer_product_test'
TABLE_CG = 'CustomerGroupings'
PROC_DB = 'stg'
# TEMP_TABLE_CRT = 'customer_prod_dummy_table_creation'
TEMP_TO_FINAL = 'CUSTOMER_PROD_TEST_TO_FINAL'
MM_PROC_CP = 'uspFactSalesMM'
MM_PROC_DB = 'edw_reporting'
CP_FINAL_TABLE = 'customer_product_test_final'

ARCHIVE_PATH_REGEX = 'ARCHIVAL_FOLDER/'
REJECTED_PATH_REGEX = 'REJECTION_FOLDER/'
UPC_PROC_CP = 'uspFactSalesUPC'
UPC_PROC_DB = 'edw_reporting'

##############################POS DETAILS###########################################################

POS_BUCKET_NAME = 'pos_unified_schema'
POS_TABLE_ID = 'DailyPOS'
POS_TEMP_TABLE_ID= 'temp_DailyPOS'
POS_PREFIX = 'NewFileProcessing/'
BATCH_PROC = 'uspBatchRunInfoUpdate'
BATCH_PROC_DATASET = 'etl'
DF_COLUMN_NAMES = ['Customer','SalesOrg','Time','DistributionChannel','FulfillmentMethod','Currency','UOM','Vendor','Store', 'RetailItemNumber','UPC',
                    'GTIN','Material', 'TCINNo','StoreCount','OnOrderQty','RetailerCost','RetailerPrice','WarehouseInv','OnhandInv', 'POSSales',
                     'POSSalesUnits','TotalInv','TotalInvValue','InStock',  'OutStock', 'unsellableOnHandUnits','unsellableOnHandUnitsAmount',
                     'sellableOnHandInventoryAmount']
                    
BQ_TABLE_SCHEMA = [
    {'name': 'user_id', 'type': 'STRING'},
    {'name': 'Sales_Organization', 'type': 'STRING'},
    {'name': 'Transaction_Date', 'type': 'DATE'},
    {'name': 'Distribution_Channel', 'type': 'STRING'},
    {'name': 'Fulfillment_Method', 'type': 'STRING'},
    {'name': 'Currency_Key', 'type': 'STRING'},
    {'name': 'Unit_of_Measure', 'type': 'STRING'},
    {'name': 'Vendor_Ref_Number', 'type': 'INTEGER'},
    {'name': 'Store_Number', 'type': 'NUMERIC'},
    {'name': 'Retailer_Item_Number', 'type': 'STRING'},
    {'name': 'UPC', 'type': 'STRING'},
    {'name': 'GTIN', 'type': 'STRING'},
    {'name': 'Material', 'type': 'STRING'},
    {'name': 'TCIN', 'type': 'STRING'},
    {'name': 'Store_Count', 'type': 'INTEGER'},
    {'name': 'On_Order_Quantity_POS', 'type': 'INTEGER'},
    {'name': 'Retailer_Cost', 'type': 'NUMERIC'},
    {'name': 'Retail_Price', 'type': 'NUMERIC'},
    {'name': 'Warehouse_Inventory_POS', 'type': 'INTEGER'},
    {'name': 'On_Hand_Inventory_POS', 'type': 'INTEGER'},
    {'name': 'POS_Sales_Dollar', 'type': 'NUMERIC'},
    {'name': 'POS_Sales_Units', 'type': 'INTEGER'},
    {'name': 'Total_Inventory_POS', 'type': 'INTEGER'},
    {'name': 'Total_Inventory_Value', 'type': 'NUMERIC'},
    {'name': 'In_Stock_Percent', 'type': 'NUMERIC'},
    {'name': 'Out_of_Stock_Percent', 'type': 'NUMERIC'},
    {'name': 'Unsellable_On_Hand_Units', 'type': 'INTEGER'},
    {'name': 'Unsellable_On_Hand_Dollar', 'type': 'NUMERIC'},
    {'name': 'Sellable_On_Hand_Inventory_Dollar', 'type': 'NUMERIC'},
    {'name': 'Suggested_Retail_Price', 'type': 'NUMERIC'},
    {'name': 'Final_Material', 'type': 'STRING'},
    {'name': 'Mapping_Source', 'type': 'STRING'}
]

# ------------------------------------_Replace-------------------------------------------
POS_PROC_DB ='edw_reporting'
POS_PROC_NAME ='uspfactsalespos'
REM_PROC_NAME = 'uspFactSalesREM'