/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/

-- u = user, first checking if this table exist then deleting and creating AGAIN!!
-- FOR ALL 6 TABLES
if object_id ('bronze.crm_cust_info', 'U') is not null
drop table bronze.crm_cust_info;
create table bronze.crm_cust_info(
cst_id int,
cst_key nvarchar(50),
cst_firstname nvarchar(50),
cst_lastname nvarchar(50),
cst_marital_status nvarchar(50), 
cst_gndr nvarchar(50),
cst_create_date date
);

if object_id ('bronze.crm_prd_info', 'U') is not null
drop table bronze.crm_prd_info;
create table bronze.crm_prd_info(
prd_id int,
prd_key nvarchar(50),
prd_nm nvarchar(50),
prd_cost int,
prd_line nvarchar(50),
prd_start_dt datetime,
prd_end_dt datetime
);

if object_id ('bronze.crm_sales_details', 'U') is not null
drop table bronze.crm_sales_details;
create table bronze.crm_sales_details(
sls_ord_num nvarchar(20),
sls_prd_key nvarchar(20),
sls_cust_id int,
sls_order_dt nvarchar(50),
sls_ship_dt nvarchar(50),
sls_due_dt nvarchar(50),
sls_sales int,
sls_quantity int,
sls_price int
);

if object_id ('bronze.erp_CUST_AZ12', 'U') is not null
drop table bronze.erp_CUST_AZ12;
create table bronze.erp_CUST_AZ12(
CID nvarchar(50),
BDATE nvarchar(50),
GEN NVARCHAR(10)
);

if object_id ('bronze.erp_LOC_A101', 'U') is not null
drop table bronze.erp_LOC_A101;
create table bronze.erp_LOC_A101(
CID NVARCHAR(50),
CNTRY NVARCHAR(50)
);

if object_id ('bronze.erp_PX_CAT_G1V2', 'U') is not null
drop table bronze.ERP_PX_CAT_G1V2;
CREATE TABLE BRONZE.ERP_PX_CAT_G1V2(
ID NVARCHAR(20),
CAT nVARCHAR(50),
SUBCAT nVARCHAR(50),
MAINTENANCE nVARCHAR(10)
);


