SELECT
   case when columns[2] = '' then cast(null as varchar(100)) else cast(columns[2] as varchar(100)) end AS ss_item_sk,
   case when columns[3] = '' then cast(null as varchar(100)) else cast(columns[3] as varchar(100)) end AS ss_customer_sk,
   case when columns[4] = '' then cast(null as varchar(100)) else cast(columns[4] as varchar(100)) end AS ss_cdemo_sk,
   case when columns[5] = '' then cast(null as varchar(100)) else cast(columns[5] as varchar(100)) end AS ss_hdemo_sk,
   case when columns[0] = '' then cast(null as varchar(100)) else cast(columns[0] as varchar(100)) end AS s_sold_date_sk,
   case when columns[8] = '' then cast(null as varchar(100)) else cast(columns[8] as varchar(100)) end AS ss_promo_sk
FROM `dfs.data`.`sort_spill/store_sales_small.dat`
ORDER BY ss_promo_sk
