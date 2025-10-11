/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'silver' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/

if object_id ('silver.crm_cust_info', 'U') is not null
drop table silver.crm_cust_info;
create table silver.crm_cust_info(
cst_id int,
cst_key nvarchar(50),
cst_firstname nvarchar(50),
cst_lastname nvarchar(50),
cst_marital_status nvarchar(50), 
cst_gndr nvarchar(50),
cst_create_date date,
dwh_create_date datetime2 default getdate()
);

if object_id ('silver.crm_prd_info', 'U') is not null
drop table silver.crm_prd_info;
create table silver.crm_prd_info(
prd_id int,
prd_key nvarchar(50),
prd_nm nvarchar(50),
prd_cost int,
prd_line nvarchar(50),
prd_start_dt datetime,
prd_end_dt datetime,
dwh_create_date datetime2 default getdate()
);

if object_id ('silver.crm_sales_details', 'U') is not null
drop table silver.crm_sales_details;
create table silver.crm_sales_details(
sls_ord_num nvarchar(20),
sls_prd_key nvarchar(20),
sls_cust_id int,
sls_order_dt date,
sls_ship_dt date,
sls_due_dt date,
sls_sales int,
sls_quantity int,
sls_price int,
dwh_create_date datetime2 default getdate()
);

if object_id ('silver.erp_CUST_AZ12', 'U') is not null
drop table silver.erp_CUST_AZ12;
create table silver.erp_CUST_AZ12(
CID nvarchar(50),
BDATE nvarchar(50),
GEN NVARCHAR(10),
dwh_create_date datetime2 default getdate()
);

if object_id ('silver.erp_LOC_A101', 'U') is not null
drop table silver.erp_LOC_A101;
create table silver.erp_LOC_A101(
CID NVARCHAR(50),
CNTRY NVARCHAR(50),
dwh_create_date datetime2 default getdate()
);

if object_id ('silver.erp_PX_CAT_G1V2', 'U') is not null
drop table silver.ERP_PX_CAT_G1V2;
CREATE TABLE silver.ERP_PX_CAT_G1V2(
ID NVARCHAR(20),
CAT nVARCHAR(50),
SUBCAT nVARCHAR(50),
MAINTENANCE nVARCHAR(10),
dwh_create_date datetime2 default getdate()
);
