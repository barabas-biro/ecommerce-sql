-- 1.Data and KPI Foundation
-- Create clean analytical table from raw dataset
CREATE TABLE orders AS 
SELECT
	`Order ID` AS order_id,
    `Order Date` AS order_date,
    `Customer Name` AS customer_name,
    `Region` AS region,
    `City` AS city,
    `Category` AS category,
    `Sub-Category` AS sub_category,
    `Product Name` AS product_name,
    `Quantity` AS quantity,
    `Unit Price` AS unit_price,
    `Discount` AS discount,
    `Sales` AS sales,
    `Profit` AS profit,
    `Payment Mode` AS payment_mode
FROM ecommerce_sales_data_2024_2025;

-- Validate uniqueness of order_id (check for duplicate orders)
SELECT order_id,
       count(*)
FROM orders
GROUP BY order_id
HAVING count(*) > 1;

-- 2.Performance Drivers
-- Top 10 customers by total sales
SELECT customer_name,
	   SUM(sales) AS total_revenue,
       COUNT(*) AS orders,
       ROUND(AVG(sales),2) AS average_revenue
FROM orders
GROUP BY customer_name
ORDER BY total_spend DESC
LIMIT 10;

-- Top 10 customers by profit
SELECT customer_name,
       ROUND(sum(profit),2) AS total_profit,
       ROUND(AVG(profit),2) AS average_profit
FROM orders
GROUP BY customer_name
ORDER BY total_profit DESC
LIMIT 10;

-- Top 10 customers by profit percentage
SELECT customer_name,
       ROUND(sum(profit) /NULLIF(SUM(sales),0)*100,2) AS profit_margin_pct
FROM orders
GROUP BY customer_name
ORDER BY profit_pct DESC
LIMIT 10;

-- Total sales by month
SELECT DATE_FORMAT(order_date, '%Y-%m') AS date_month,
       ROUND(SUM(sales),2) AS monthly_revenue
FROM orders
GROUP BY date_month
ORDER BY date_month ASC;

-- Monthly revenue by region
SELECT region,
       DATE_FORMAT(order_date, '%Y-%m') AS date_month,
       ROUND(SUM(sales),2) AS monthly_region_revenue
FROM orders
GROUP BY REGION, date_month 
ORDER BY date_month ASC, region;

-- Monthly revenue by category
SELECT category,
	   DATE_FORMAT(order_date, '%Y-%m') AS date_month,
       ROUND(SUM(sales),2) AS monthly_category_revenue
FROM orders
GROUP BY category, date_month
ORDER BY date_month ASC, category;

-- Category performance
SELECT category,
	   ROUND(SUM(sales),2) AS total_revenue,
       ROUND(SUM(profit),2) AS total_profit,
       ROUND(sum(profit) /NULLIF(SUM(sales),0)*100,2) AS profit_margin_pct
FROM orders
GROUP BY category
ORDER BY total_profit DESC;

-- Sub-Category performance
SELECT sub_category,
	   ROUND(SUM(sales),2) AS total_revenue,
       ROUND(SUM(profit),2) AS total_profit,
       ROUND(sum(profit) /NULLIF(SUM(sales),0)*100,2) AS profit_margin_pct
FROM orders
GROUP BY sub_category
ORDER BY total_profit DESC;

-- Product performance
SELECT product_name,
	   ROUND(SUM(sales),2) AS total_revenue,
       ROUND(SUM(profit),2) AS total_profit,
       ROUND(sum(profit) /NULLIF(SUM(sales),0)*100,2) AS profit_margin_pct
FROM orders
GROUP BY product_name
ORDER BY total_profit DESC
LIMIT 20;

-- 3.Trend Analysis
-- Top 5 products by sub-categories
SELECT category,
       sub_category,
       rnk,
       product_name,
       total_revenue,
       total_profit,
       profit_margin_pct
FROM (SELECT category,
           sub_category,
           product_name,
           total_revenue,
           total_profit,
           profit_margin_pct,
           DENSE_RANK() OVER (PARTITION BY sub_category ORDER BY total_revenue DESC) AS rnk,
           MAX(total_revenue) OVER (PARTITION BY sub_category) AS subcat_top_revenue
           FROM (SELECT category,
               sub_category,
               product_name,
               ROUND(SUM(sales),2) AS total_revenue,
               ROUND(SUM(profit),2) AS total_profit,
               ROUND(SUM(profit) / NULLIF(SUM(sales),0) * 100,2) AS profit_margin_pct
          FROM orders
          GROUP BY category, sub_category, product_name) AS aggregated
) AS ranked
WHERE rnk <= 5
ORDER BY subcat_top_revenue DESC, sub_category, rnk;

-- Product - sub-category percentage
SELECT category,
	   sub_category,
       rnk,
       product_name,
       total_revenue,
       ROUND(subcat_total_revenue,2) AS subcat_total_revenue,
       ROUND((total_revenue/NULLIF(subcat_total_revenue,0))*100,2) AS product_revenue_share_pct
FROM (SELECT category,
			 sub_category,
             product_name,
             total_revenue,
            SUM(total_revenue) OVER(PARTITION BY sub_category) AS subcat_total_revenue,
             DENSE_RANK() OVER(PARTITION BY sub_category ORDER BY total_revenue DESC)  AS rnk,
             MAX(total_revenue) OVER (PARTITION BY sub_category) AS subcat_top_revenue
	 FROM (SELECT category,
               sub_category,
               product_name,
               ROUND(SUM(sales),2) AS total_revenue
          FROM orders
          GROUP BY category, sub_category, product_name) AS aggregated
	 ) AS ranked
WHERE rnk <= 5
ORDER BY subcat_top_revenue DESC, sub_category, rnk;

-- Pareto Analysis(80/20 Rule)
SELECT product_name,
	   category,
       sub_category,
       total_revenue,
       ROUND(running_total,2) AS running_total,
       ROUND(cumulative_pct,2) AS cumulative_pct
FROM (SELECT product_name,
	   category,
       sub_category,
       total_revenue,
       SUM(total_revenue) OVER (ORDER BY total_revenue DESC) AS running_total,
       SUM(total_revenue) OVER () AS total_revenue_all,
       (SUM(total_revenue) OVER (ORDER BY total_revenue DESC)/NULLIF(SUM(total_revenue) OVER (),0))*100 AS cumulative_pct
       FROM (SELECT product_name,
	                category,
                    sub_category,
                    SUM(sales) AS total_revenue
			FROM orders
            GROUP BY product_name, category, sub_category) AS aggregated
       ) AS pareto_data
WHERE cumulative_pct <=80
ORDER BY total_revenue DESC;
	
-- Contribution Analysis
SELECT category,
       sub_category,
       rnk,
       product_name,
       total_revenue,
       ROUND(category_total_revenue,2) AS category_total_revenue,
       ROUND(contribution_pct,2) AS contribution_pct
FROM (SELECT product_name,
	   category,
       sub_category,
       ROUND(total_revenue,2) AS total_revenue,
       SUM(total_revenue) OVER ( PARTITION BY category ) AS category_total_revenue,
       (total_revenue/NULLIF(SUM(total_revenue) OVER (PARTITION BY category),0))*100 AS contribution_pct,
       DENSE_RANK() OVER (PARTITION BY category ORDER BY total_revenue DESC) AS rnk
       FROM (SELECT product_name,
	                category,
                    sub_category,
                    SUM(sales) AS total_revenue
			FROM orders
            GROUP BY product_name, category, sub_category) AS aggregated
       ) AS pareto_data
WHERE rnk<=10
ORDER BY category_total_revenue DESC,category, rnk ASC, total_revenue DESC ;

-- Category Performance summary
SELECT category,
	   ROUND(category_total_revenue,2) AS category_total_revenue,
       ROUND(category_total_profit,2) AS category_total_profit,
       ROUND(category_total_profit/NULLIF(category_total_revenue,0)*100,2) AS profit_margin_pct,
       ROUND(category_total_profit/NULLIF(SUM(category_total_profit) OVER(),0)*100,2) AS profit_share_pct,
       ROUND(category_total_revenue/NULLIF(SUM(category_total_revenue) OVER(),0)*100,2) AS revenue_share_pct
FROM (SELECT category,
			 SUM(sales) AS category_total_revenue,
			 SUM(profit) AS category_total_profit
	 FROM orders
	 GROUP BY category) AS aggregated
ORDER BY category_total_profit DESC;

-- Monthly trend analysis
SELECT category, 
       DATE_FORMAT(order_date, '%Y-%m') AS date_month,
       ROUND(SUM(sales),2) AS monthly_category_revenue,
       ROUND(SUM(profit),2) AS monthly_category_profit
FROM orders
GROUP BY date_month,category
ORDER BY date_month, category;
	   



       
	
        