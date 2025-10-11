/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

create or alter procedure silver.load_silver as 
begin
	declare @starttime datetime, @endtime datetime, @batchstarttime datetime, @batchendtime datetime;
	begin try
		set @batchstarttime = getdate();
		print '---------------------------------------'
		print 'Truncating table : silver.crm_cust_info'
		truncate table silver.crm_cust_info
		set @starttime = getdate();
		print 'Inserting into table : silver.crm_cust_info'
		insert into silver.crm_cust_info(
		cst_id, 
		cst_key, 
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date)		

		select
		[cst_id] 
		,[cst_key]
		,trim([cst_firstname]) cst_firstname
		,trim([cst_lastname]) cst_lastname
		,case when upper(trim(cst_marital_status)) = 'S' then 'Single'
			  when upper(trim(cst_marital_status)) = 'M' then 'Married'
			  else 'n/a' 
			  end as cst_marital_status
		,case when upper(trim([cst_gndr])) = 'F' then 'Female'
			  when upper(trim([cst_gndr])) = 'M' then 'Male'
			  else 'n/a' 
			  end as [cst_gndr]
		,[cst_create_date]
		from (select * ,
		ROW_NUMBER() over (partition by cst_id order by cst_create_date desc) as rownum
		from bronze.crm_cust_info)t
		where rownum = 1 and cst_id is not null;
		set @endtime = GETDATE();
		print 'Duration taken for silver.crm_cust_info:' + cast(datediff(second, @starttime, @endtime) as nvarchar) + 'seconds'
		print '---------------------------------------'

		set @starttime = getdate();
		print '---------------------------------------'
		print 'Truncating table : silver.crm_prd_info'
		truncate table silver.crm_prd_info
		print 'Inserting into table : silver.crm_prd_info'
		insert into silver.crm_prd_info
		(prd_id,
		cat_id ,
		prd_key , 
		prd_nm , 
		prd_cost , 
		prd_line ,
		prd_start_dt,
		prd_end_date
		)

		SELECT prd_id
			  ,replace(substring(prd_key, 1, 5), '-', '_') as cat_id
			  ,substring(prd_key, 7, len(prd_key)) as prd_key
			  ,[prd_nm]
			  ,isnull([prd_cost], 0) as prd_cost
			  ,
			  case upper(trim(prd_line)) 
					when 'S' then 'Other Sales'
					when 'M' then 'Mountain'
					when 'R' then 'Road'
					when 'T' then 'Touring'
					else 'n/a'
				end as prd_line, 
				cast(prd_start_dt as date) as prd_start_dt, 
				CAST(DATEADD(DAY, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS DATE) AS prd_end_date
				-- DATEADD() adds or subtracts time from a date. 
				-- DATEADD(interval, number, date)

		from bronze.crm_prd_info;
		set @endtime = GETDATE();
		print 'Duration taken for silver.crm_prd_info:' + cast(datediff(second, @starttime, @endtime) as nvarchar) + 'seconds'
		print '---------------------------------------'

		print '---------------------------------------'
		set @starttime = getdate();
		print 'Truncating table : silver.crm_sales_details'
		truncate table silver.crm_sales_details
		print 'Inserting into table : silver.crm_sales_details'
		insert into silver.crm_sales_details(
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price)
		SELECT [sls_ord_num]
			  ,[sls_prd_key]
			  ,[sls_cust_id]
			  , case when sls_order_dt < = 0 or len(sls_order_dt) != 8 then Null
				else cast(cast(sls_order_dt as varchar) as date)
				end as sls_order_dt
			  , case when sls_ship_dt < = 0 or len(sls_ship_dt) != 8 then Null
				else cast(cast(sls_ship_dt as varchar) as date)
				end as sls_ship_dt
			  , case when sls_due_dt < = 0 or len(sls_due_dt) != 8 then Null
				else cast(cast(sls_due_dt as varchar) as date)
				end as sls_due_dt
			  , case when sls_sales <= 0 or sls_sales is null or sls_sales != sls_quantity* abs(sls_price)
			  then sls_quantity* abs(sls_price)
			  else sls_sales
			  end as sls_sales
			  ,[sls_quantity]
			  ,case when sls_price <= 0 or sls_price is null
			  then abs(sls_price)
			  else sls_price
			  end as sls_price
		FROM [DataWarehouse].[bronze].[crm_sales_details];
		set @endtime = GETDATE();
		print 'Duration taken for silver.crm_sales_details:' + cast(datediff(second, @starttime, @endtime) as nvarchar) + 'seconds'
		print '---------------------------------------'

		print '---------------------------------------'
		set @starttime = getdate();
		print 'Truncating table : silver.erp_CUST_AZ12'
		truncate table silver.erp_CUST_AZ12

		print 'Inserting into table : silver.erp_CUST_AZ12'
		insert into silver.erp_CUST_AZ12(cid, bdate, gen)

		SELECT case when cid like 'NASA%' THEN SUBSTRING(CID, 4, LEN(CID))
				ELSE CID
			   END AS CID
			  , CASE WHEN BDATE > GETDATE() THEN NULL ELSE BDATE END AS BDATE 
			  , CASE  WHEN upper(trim(GEN)) LIKE 'M%' then 'Male'
					  when upper(trim(gen)) like 'F%' then 'Female'
					  else 'n/a'
			  end as gen
		FROM [DataWarehouse].[bronze].[erp_CUST_AZ12];
		set @endtime = GETDATE();
		print 'Duration taken for silver.erp_CUST_AZ12:' + cast(datediff(second, @starttime, @endtime) as nvarchar) + 'seconds'
		print '---------------------------------------'

		print '---------------------------------------'
		set @starttime = getdate();
		print 'Truncating table : silver.erp_LOC_A101'
		truncate table silver.erp_LOC_A101
		print 'Inserting into table : silver.erp_LOC_A101'
		insert into silver.erp_LOC_A101(cid, cntry)
		SELECT replace(cid, '-', '') cid
			  , case when UPPER(TRIM(cntry)) in ('US', 'USA') THEN 'United States'
					when UPPER(TRIM(cntry)) = 'DE' then 'Germany'
					when UPPER(TRIM(cntry)) = '' or cntry is null then 'n/a'
					else (TRIM(cntry))
					end as cntry
		FROM [DataWarehouse].[bronze].[erp_LOC_A101];
		set @endtime = GETDATE();
		print 'Duration taken for silver.crm_cust_info:' + cast(datediff(second, @starttime, @endtime) as nvarchar) + 'seconds'
		print '---------------------------------------'

		print '---------------------------------------'
		set @starttime = getdate();
		print 'Truncating table : silver.ERP_PX_CAT_G1V2'
		truncate table silver.ERP_PX_CAT_G1V2
		print 'Inserting into table : silver.ERP_PX_CAT_G1V2'
		insert into silver.ERP_PX_CAT_G1V2(id, cat, subcat, MAINTENANCE)
		SELECT [ID]
			  ,[CAT]
			  ,[SUBCAT]
			  ,[MAINTENANCE]
		FROM [DataWarehouse].[bronze].[ERP_PX_CAT_G1V2];
		set @endtime = GETDATE();
		print 'Duration taken for silver.crm_cust_info:' + cast(datediff(second, @starttime, @endtime) as nvarchar) + 'seconds'
		print '---------------------------------------'
		set @batchendtime = getdate();
		print '>> Batch Load duration:' + cast(datediff(millisecond, @batchstarttime, @batchendtime) as nvarchar) + ' milliseconds';

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

exec silver.load_silver
