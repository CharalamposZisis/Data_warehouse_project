/*
============================================
Create Gold View
===========================================
Script Purpose:
  This script creates views for the Gold layer in the data warehouse. 
  The gold Layer represents the final dimension and fact tables (star Schema)
  
  Each view perfors transformations and combines data from the Silver layer to produce a clean, 
  enriched and business-ready dataset.
*/

create view gold.dim_customers as 
SELECT 
	ROW_NUMBER() over (order by ci.cst_id) as customer_key, -- This called surrogate key 
	ci.cst_id as customer_id
	,ci.cst_key as customer_number
	,ci.cst_firstname as firstname
	,ci.cst_lastname as lastname
	,loc.cntry as country
	,ci.cst_marital_status as marital_status
	,case when ci.cst_gndr != 'n/a' then ci.cst_gndr
		else coalesce(ca.gen,'n/a') -- we keep the gender info from the crm_cust_info
	end as new_gen    
	,ci.cst_create_date as create_date
	,ca.bdate as birthdate
FROM .silver.crm_cust_info as ci
left join silver.erp_cust_az12 as ca
on ca.cid = ci.cst_key
left join silver.erp_loc_a101 as loc
on loc.cid=ci.cst_key


create view gold.dim_products as 
SELECT  
ROW_NUMBER() over (order by pn.prd_start_dt, pn.prd_key) as product_key,
pn.prd_id as product_id
,pn.prd_nm as product_name
,pn.prd_key as product_number
,pn.cat_id as category_id
,cg.cat as category
,cg.subcat as subcategory
,cg.maintenance as maintenance
,pn.prd_cost as product_cost
,pn.prd_line as product_line
,pn.prd_start_dt as start_date
FROM silver.crm_prd_info as pn
left join silver.erp_px_cat_g1v2 as cg
on pn.cat_id = cg.id
where pn.prd_end_dt is null -- filter that our data are current


-- Create the gold.fact_sales view for fact sales

create view gold.fact_sales as 
SELECT 
sl.sls_ord_num as order_number
,pr.product_key
,dc.customer_key
,sl.sls_order_dt as order_date
,sl.sls_ship_dt as shipping_date
,sl.sls_due_dt as due_date
,sl.sls_sales as sales_amount
,sls_quantity as quantity
,sl.sls_price as price
FROM silver.crm_sales_details as sl
left join gold.dim_products as pr
on pr.product_number = sl.sls_prd_key
left join gold.dim_customers as dc
on dc.customer_id = sl.sls_cust_id



