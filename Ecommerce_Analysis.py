#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import mysql.connector


# In[3]:


# Connect to the MySQL database
db = mysql.connector.connect(
    host='localhost',
    user='root',
    password='12345',
    database='ecommerce'
)


# In[4]:


pip install mysql-connector-python


# In[5]:


cursor = db.cursor()


# # List all unique cities where customers are located.

# In[6]:


query1="""select distinct customer_city from customers"""
cursor.execute(query1)

#Fetch the result and store in data variable
data1=cursor.fetchall()
print(data1)


# # Count the number of orders placed in 2017.

# In[7]:


query2="""select count(order_id) from orders where year(order_purchase_timestamp)=2017"""
cursor.execute(query2)
data2=cursor.fetchall()

print("number of orders placed",data2[0][0])


# # Find the total sales per category.

# In[8]:


query3=""" """

cursor.execute(query3)
data3=cursor.fetchall()

df=pd.DataFrame(data3,columns=['Category','Sales'])
print(df)


# # Calculate the percentage of orders that were paid in installments.

# In[9]:


query4="""select (sum(case when payment_installments>=1 then 1 else 0 end)/count(*))*100 from payments"""
cursor.execute(query4)
data4=cursor.fetchall()

print("percentage of orders that were paid in installments",data4[0][0])


# # Count the number of customers from each state.

# In[53]:


query5="""select customer_state,count(customer_id) tot_customer from customers group by customer_state """
cursor.execute(query5)
data5=cursor.fetchall()

df=pd.DataFrame(data5,columns=['state','Total_Customers'])
print(df)

plt.bar(df["state"],df["Total_Customers"],color='orange')
plt.xticks(rotation=90)
plt.figure(figsize=(9,4))
plt.show()


# # Calculate the number of orders per month in 2018.

# In[55]:


query6="""select month(order_purchase_timestamp) month,count(order_id) number_orders from orders 
        where year(order_purchase_timestamp)=2018 group by month """
cursor.execute(query6)
data6=cursor.fetchall()
o=['January','February','March','April','May','June','July','August','September','October','November','December']
df=pd.DataFrame(data6,columns=['month','number_orders'])

# Replace numerical months with their names
df['month'] = df['month'].apply(lambda x: o[x-1])  # Convert month numbers to names

# Ensure the months are ordered correctly
df['month'] = pd.Categorical(df['month'], categories=o, ordered=True)
df = df.sort_values('month')
plt.figure(figsize=(9,4))
plt.bar(df['month'], df['number_orders'], color='orange')
plt.xticks(rotation=45)  # Rotate x-axis labels for better readability
plt.title("Number of Orders per Month in 2018")
plt.xlabel("Month")
plt.ylabel("Number of Orders")
plt.show()
print(df)


# # Find the average number of products per order, grouped by customer city.

# In[25]:


query7="""with count_per_order as (select orders.order_id,orders.customer_id,count(order_items.order_id) co
from orders  join order_items 
on orders.order_id=order_items.order_id
group by orders.order_id,orders.customer_id)
select customers.customer_city,round(avg(count_per_order.co),2) from customers join count_per_order on customers.customer_id=count_per_order.customer_id
group by customer_city"""

cursor.execute(query7)
data7=cursor.fetchall()

df=pd.DataFrame(data7,columns=['Customer_City','Average_Order'])
print(df)


# # Calculate the percentage of total revenue contributed by each product category.

# In[27]:


query8="""select p.product_category category,round(sum(pay.payment_value),2) sale,concat(round((sum(pay.payment_value)/(select sum(payment_value) from payments))*100,2),'%') percentage_revenue from
products p join order_items o 
on p.product_id=o.product_id
join payments pay on pay.order_id=o.order_id
group by category"""

cursor.execute(query8)
data8=cursor.fetchall()

df=pd.DataFrame(data8,columns=['Category','Sale','Percentage_Revenue'])
print(df.head(10))


# # Identify the correlation between product price and the number of times a product has been purchased.

# In[32]:


query9="""select products.product_category,count(order_items.product_id),round(avg(order_items.price+order_items.freight_value),2) average_price
from products join order_items
on products.product_id=order_items.product_id
group by products.product_category"""

cursor.execute(query9)
data9=cursor.fetchall()

df=pd.DataFrame(data9,columns=['Product_Category','total_number_purchased','average_price'])
print(df.head(10))

# Step 4: Plot the correlation graph
plt.figure(figsize=(10, 6))
sns.regplot(x=df["average_price"], y=df["total_number_purchased"], data=df, scatter_kws={"color": "blue"}, line_kws={"color": "red"})
plt.title("Correlation between Product Price and Number of Purchases")
plt.xlabel("Average Product Price")
plt.ylabel("Number of Purchases")
plt.grid(True)
plt.show()

# Step 5: Display the correlation coefficient
correlation = df['average_price'].corr(df['total_number_purchased'])
print(f"Correlation Coefficient: {correlation}")


# # Calculate the total revenue generated by each seller, and rank them by revenue.

# In[36]:


query10="""select *, dense_rank() over(order by total_revenue desc) as rn from (SELECT 
    order_items.seller_id,
    ROUND(SUM(payments.payment_value), 2) AS total_revenue
FROM order_items join payments
on order_items.order_id = payments.order_id
GROUP BY order_items.seller_id
ORDER BY total_revenue DESC) as res"""

cursor.execute(query10)
data10=cursor.fetchall()

df=pd.DataFrame(data10,columns=['seller_id','total_revenue','rank'])
# df['Rank'] = df['total_revenue'].rank(method='dense', ascending=False).astype(int)
# Display the result
# print("Total Revenue by Seller (Ranked):\n")
print(df)

# print(df.head(10))


# # Find Top 5 & Below 5 seller

# In[39]:


top=df.head(5)  #Top5 sellers
below=df.tail(5)  #Below5 sellers

print("Top5 Selllers")
print(top)
print("\t")
print("Below5 Selllers")
print(below)


# # Calculate the moving average of order values for each customer over their order history.

# In[41]:


query11="""select customer_id,order_purchase_timestamp,payment,
round(avg(payment) over(partition by customer_id order by order_purchase_timestamp rows between 2 preceding and current row),2) as mov_avg from 
(select orders.customer_id,orders.order_purchase_timestamp,payments.payment_value as payment
from payments join orders
on payments.order_id=orders.order_id) as res"""

cursor.execute(query11)
data11=cursor.fetchall()

df=pd.DataFrame(data11,columns=["customer_id","order_purchase_timestamp","payment","mov_avg"])
df


# # Calculate the cumulative sales per month for each year.
# 

# In[42]:


query12="""SELECT 
    YEAR(orders.order_purchase_timestamp) AS order_year,
    MONTHNAME(orders.order_purchase_timestamp) AS order_month,
    round(SUM(payments.payment_value),2) AS monthly_sales,
    round(SUM(SUM(payments.payment_value)) OVER (
        PARTITION BY YEAR(orders.order_purchase_timestamp)
        ORDER BY MONTH(orders.order_purchase_timestamp)
    ),2) AS cumulative_sales
FROM 
    payments
JOIN 
    orders ON payments.order_id = orders.order_id
GROUP BY 
    YEAR(orders.order_purchase_timestamp),
    MONTH(orders.order_purchase_timestamp),
    MONTHNAME(orders.order_purchase_timestamp)
ORDER BY 
    YEAR(orders.order_purchase_timestamp), 
    MONTH(orders.order_purchase_timestamp);"""

cursor.execute(query12)
data12=cursor.fetchall()

df=pd.DataFrame(data12,columns=["order_year","order_month","monthly_sales","cumulative_sales"])
df


# # Calculate the year-over-year growth rate of total sales.

# In[44]:


query13="""with current_sales_year as (select year(orders.order_purchase_timestamp) order_year,
round(sum(payments.payment_value),2) as total_sales
from payments join orders
on payments.order_id=orders.order_id
group by year(orders.order_purchase_timestamp)),
previous_sales_year as (select order_year,total_sales,round(lag(total_sales) over (order by order_year),2) as prev_sales
from current_sales_year) select order_year,total_sales,prev_sales,
case
	when prev_sales is not null then
    round((total_sales-prev_sales)/prev_sales*100,2)
    Else
		Null
	End as YOY_Growth_rate
from previous_sales_year"""

cursor.execute(query13)
data13=cursor.fetchall()

df=pd.DataFrame(data13,columns=["order_year","Current_Year_Sales","Previous_Year_Sales","YOY_Growth_Rate"])
df


# # Calculate the retention rate of customers, defined as the percentage of customers who make another purchase within 6 months of their first purchase.

# In[56]:


query14="""WITH first_purchases AS (
    SELECT
        o.customer_id,
        MIN(o.order_purchase_timestamp) AS first_purchase_date
    FROM orders o
    GROUP BY o.customer_id
),
subsequent_purchases AS (
    SELECT
        fp.customer_id,
        COUNT(o.order_id) AS repeat_purchases
    FROM first_purchases fp
    JOIN orders o
        ON fp.customer_id = o.customer_id
        AND o.order_purchase_timestamp > fp.first_purchase_date
        AND o.order_purchase_timestamp <= DATE_ADD(fp.first_purchase_date, INTERVAL 6 MONTH)
    GROUP BY fp.customer_id
),
retention_stats AS (
    SELECT
        COUNT(DISTINCT fp.customer_id) AS total_customers,
        COUNT(DISTINCT sp.customer_id) AS retained_customers
    FROM first_purchases fp
    LEFT JOIN subsequent_purchases sp
        ON fp.customer_id = sp.customer_id
)
SELECT
    total_customers,
    retained_customers,
    (retained_customers * 100.0 / total_customers) AS retention_rate
FROM retention_stats"""

cursor.execute(query14)
data14=cursor.fetchall()

df=pd.DataFrame(data14,columns=["total_customers","retained_customers","retention_rate"])
df


# # Identify the top 3 customers who spent the most money in each year.

# In[57]:


query15="""select years,customer_id,d_rank
from
(select year(orders.order_purchase_timestamp) years,
orders.customer_id,
sum(payments.payment_value) as payment,
dense_rank() over (partition by year(orders.order_purchase_timestamp) 
order by sum(payments.payment_value) desc) d_rank
from payments join orders
on payments.order_id=orders.order_id
group by year(orders.order_purchase_timestamp),
orders.customer_id) as res
where d_rank<=3"""

cursor.execute(query15)
data15=cursor.fetchall()

df=pd.DataFrame(data15,columns=["Year","customer_id","d_rank"])
df


# In[ ]:




