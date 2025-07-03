# Python-SQL-1

## Dataset
Data yang digunakan berasal dari Kaggle :

- **Dataset Link:** [retail_orders](https://www.kaggle.com/datasets/ankitbansal06/retail-orders)

## Schema Data Cleaning

### 1.read data from files and handle null values
```python
import pandas as pd
df = pd.read_csv("C:/Users/ACER/Downloads/orders.csv", na_values=['Not Available', 'unknown', 'nan'])
df['Ship Mode'].unique()
```

note : 
- #na_values : Artinya kamu memberitahu pandas bahwa nilai 'Not Available' dan 'unknown' dalam file CSV harus diperlakukan sebagai missing values (NaN).

### 2. rename columns names --> make them lower case and replace space with underscore 
```python
df = df.rename(columns={'Order Id':'order_id', 'Order Date':'order_date', 'Ship Mode':'ship_mode', 'Segment':'segment', 'Country':'country', 'City':'city', 'State':'state', 'Postal Code':'postal_code', 'Region':'region', 'Category':'category', 'Sub Category':'sub_category', 'Product Id':'product_id', 'cost price':'cost_price', 'List Price':'list_price', 'Quantity':'quantity', 'Discount Percent':'discount_percent'})
```

gunakan cara ini biar lebih simple

```python
df.columns 
df.columns = df.columns.str.lower()
df.columns = df.columns.str.replace(' ', '_')
df
```

### 3. derive new columns discount, sale price and profit
```python
df['discount']=df['list_price']*df['discount_percent']*.01
df['sale_price'] = df['list_price'] - df['discount']
df['profit'] = df['sale_price'] - df['cost_price']
df
```

### 4. convert order date from objet data type to date time
```python
df.dtypes
df['order_date']=pd.to_datetime(df['order_date'], format='%Y-%m-%d')
```

### 5. drop cost price, list price & discount percent columns
```python
df.drop(columns=['list_price', 'cost_price', 'discount_percent'], inplace=True)
```

### 6.load the data into sql server using replae option (tdk direkomendasikan)
```python
import sqlalchemy as sal
engine = sal.create_engine('mssql://LAPTOP-FVQN2OFH\SQLEXPRESS/linda?driver=ODBC+DRIVER+17+FOR+SQL+SERVER')
conn=engine.connect()
```

### opsi lain : load the data into sql server using append option
```python
df.to_sql('df_orders', con=con, index=False, if_exists='append')
```

## Schema Analyst with SQL

### 1. find top 10 highest revenue generating products : di mysql pake limit klw disini select top 10
```sql
select top 10 product_id, sum(sale_price) as sales
from order_clean
group by product_id
order by 2 desc
```
![image](https://github.com/user-attachments/assets/baacb629-6f45-40aa-8d58-787d3f218a78)

### 2. find top 5 highest selling product in each region
```sql
with cte as
(
select product_id, region, sum(sale_price) as total_sales
from order_clean
group by product_id, region
)
select * from (
select *,
row_number() over(partition by region order by total_sales desc) as rn
from cte) A
where rn <= 5
```
![image](https://github.com/user-attachments/assets/c2de5a70-2534-4062-a5eb-dd2ba01eca82)

### 3. find month over month growth comparison for 2022 and 2023 sales eg : jan 2022 vs jan 2023
```sql
with cte as
(
select year(order_date) as order_year, month(order_date) as order_month, sum(sale_price) as sales
from order_clean
group by year(order_date), month(order_date)
--order by year(order_date), month(order_date)
)
select order_month
,sum(case when order_year = 2022 then sales else 0 end) as sales_2022
,sum(case when order_year = 2023 then sales else 0 end) as sales_2023
from cte
group by order_month
order by order_month
```
![image](https://github.com/user-attachments/assets/8b39e5e9-1c56-411f-b306-27f600f8813c)


### 4. for each category which month had highest sales
```sql
with cte as 
(
select category, format(order_date, 'yyyyMM') as order_year_month, sum(sale_price) as sales
from order_clean
group by category, format(order_date, 'yyyyMM')
--order by category, format(order_date, 'yyyyMM')
)
select * from
(
select *,
row_number() over(partition by category order by sales desc) as rn
from cte
) new
where rn = 1
```
![image](https://github.com/user-attachments/assets/c05c69b3-093a-428b-a030-2b143a02b6af)


### 5. which sub category had highest growth by profit in 2023 compare 2022
```sql
with cte as
(
select sub_category, year(order_date) as order_year, sum(sale_price) as sales
from order_clean
group by sub_category, year(order_date)
--order by year(order_date), month(order_date)
)
,cte2 as
(
select sub_category
,sum(case when order_year = 2022 then sales else 0 end) as sales_2022
,sum(case when order_year = 2023 then sales else 0 end) as sales_2023
from cte
group by sub_category
-- order by sub_category
)
select *, (sales_2023-sales_2022) as peningkatan
from cte2
order by peningkatan desc
```

![image](https://github.com/user-attachments/assets/d02b6c32-bcec-4ead-944a-bcd69cbc4f8b)
