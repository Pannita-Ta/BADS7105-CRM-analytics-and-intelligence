with customer_data as 
(select distinct cust_code,
DATE_TRUNC(PARSE_DATE("%Y%m%d", CAST(SHOP_DATE AS STRING)), Month) as this_month,
from `bads7105-313104.Supermarketdata.Supermarketdata`),

customer_month as 
(select cust_code, this_month, LAG(this_month,1) OVER (PARTITION BY cust_code ORDER BY this_month) AS last_month 
FROM customer_data),

Report1 as 
(SELECT cust_code, this_month, last_month,
CASE WHEN DATE_DIFF (this_month, last_month, MONTH) = 1 THEN 'CUST_REPEAT' 
    WHEN DATE_DIFF(this_month, last_month, MONTH) > 1 THEN 'CUST_REACTIVATED'
    WHEN DATE_DIFF(this_month, last_month, MONTH) IS NULL THEN 'CUST_NEW' End as Status
FROM customer_month),

Report2 as
(select cust_code, this_month, DATE_ADD(this_month, INTERVAL 1 MONTH) as Next_month,
case when this_month <= (select MAX(this_month) from customer_data) then 'CUST_CHURN' end as status 

from (select cust_code, this_month, ROW_NUMBER() OVER ( PARTITION BY cust_code ORDER BY this_month DESC ) as rwn
from customer_data) t where rwn = 1)

select this_month, status, count(Cust_code) from(
select cust_code, this_month, status 
from Report1

UNION ALL 
select cust_code, Next_month, status
from Report2 where Next_month <= (select MAX(this_month) from customer_data))

group by this_month, status

