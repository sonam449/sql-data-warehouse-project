/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/

go
create or alter procedure bronze.load_bronze as
begin
	DECLARE @start_time DATETIME, @end_time DATETIME, @start_time_batch datetime, @end_time_batch datetime
	
	BEGIN TRY
	set @start_time_batch = getdate();
		print '---------------------------------------------'
		print '==========Loading bronze layer data=========='
		print '---------------------------------------------'
		set @start_time = GETDATE();
		-- truncate and bulk inserting the data
		print '>> Truncating table: crm_cust_info >>'
		truncate table bronze.crm_cust_info;

		print '>> Inserting data into table: crm_cust_info >>'
		bulk insert bronze.crm_cust_info
		from 'D:\N_downloads\portfolio_project\data_with_baraa\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with (
			firstrow = 2, 
			fieldterminator = ',',
			tablock
		);
		set @end_time = GETDATE();
		print '>> Load duration:' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';


		set @start_time = GETDATE();
		print '>> Truncating table: crm_prd_info >>'
		truncate table bronze.crm_prd_info;
		print '>> Inserting data into table: crm_prd_info >>'
		bulk insert bronze.crm_prd_info
		from 'D:\N_downloads\portfolio_project\data_with_baraa\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with (
			firstrow = 2, 
			fieldterminator = ',',
			tablock
		);
		set @end_time = GETDATE();
		print '>> Load duration:' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';


		set @start_time = GETDATE();
		print '>> Truncating table: crm_sales_details >>'
		truncate table bronze.crm_sales_details;
		print '>> Inserting data into table: crm_sales_details >>'
		bulk insert bronze.crm_sales_details
		from 'D:\N_downloads\portfolio_project\data_with_baraa\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with (
			firstrow = 2, 
			fieldterminator = ',',
			tablock
		);
		set @end_time = GETDATE();
		print '>> Load duration:' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';

					-- Verifying table name and schema as datatype was int earlier so showing error but now changed to nvarchar()
					-- SELECT *
					-- FROM INFORMATION_SCHEMA.TABLES
					-- WHERE TABLE_SCHEMA = 'bronze' AND TABLE_NAME = 'crm_sales_details';

		set @start_time = GETDATE();
		print '>> Truncating table: erp_CUST_AZ12 >>'
		truncate table bronze.erp_CUST_AZ12;
		print '>> Inserting data into table: erp_CUST_AZ12 >>'
		bulk insert bronze.erp_CUST_AZ12
		from 'D:\N_downloads\portfolio_project\data_with_baraa\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		with (
			firstrow = 2, 
			fieldterminator = ',',
			tablock
		);
		set @end_time = GETDATE();
		print '>> Load duration:' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';

		set @start_time = GETDATE();
		print '>> Truncating table: erp_LOC_A101 >>'
		truncate table bronze.erp_LOC_A101;
		print '>> Inserting data into table: erp_LOC_A101 >>'
		bulk insert bronze.erp_LOC_A101
		from 'D:\N_downloads\portfolio_project\data_with_baraa\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		with (
			firstrow = 2, 
			fieldterminator = ',',
			tablock
		);
		set @end_time = GETDATE();
		print '>> Load duration:' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';
		

		set @start_time = GETDATE();
		print '>> Truncating table: erp_PX_CAT_G1V2 >>'
		truncate table bronze.erp_PX_CAT_G1V2;
		print '>> Inserting data into table: erp_PX_XAT_G1V2 >>'
		bulk insert bronze.erp_PX_CAT_G1V2
		from 'D:\N_downloads\portfolio_project\data_with_baraa\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		with (
			firstrow = 2, 
			fieldterminator = ',',
			tablock
		);
		set @end_time = GETDATE();
		print '>> Load duration:' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';

	set @end_time_batch = getdate();
	print '>> Batch Load duration:' + cast(datediff(millisecond, @start_time_batch, @end_time_batch) as nvarchar) + ' milliseconds';

	-- creating procedure for above data bulk insert as we need to run data on daily basis!!
	END TRY
	begin catch
	PRINT '====================================='
	print 'ERROR OCCURED DURING THE LOAD DATA'
	PRINT 'ERROR MESSAGE' + CAST(ERROR_NUMBER() AS NVARCHAR);
	PRINT 'ERROR MESSAGE' + CAST(ERROR_STATE() AS NVARCHAR);
	PRINT '====================================='
	end catch
	 
end
go


