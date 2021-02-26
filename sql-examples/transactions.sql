/*
Count all the users matching the following criteria:
    Have multiple transactions
    Have at least one transaction that was made within 7 days from the previous transaction
In:
    Public table with samle transactions data: analytics-230012.Assignments.orders
*/

WITH
data AS (
  SELECT 
    *, 
    COUNT(*) OVER (PARTITION BY userId) AS numberOfTransactions,
    ROW_NUMBER() OVER (PARTITION BY userId ORDER BY date DESC) AS transactionNumber,
    LAG(CAST(date AS TIMESTAMP), 1) OVER (PARTITION BY userId ORDER BY date) AS previousTransactionDate
  FROM `analytics-230012.Assignments.orders`)  
SELECT 
  count(userId) AS usersTotal
FROM data
WHERE numberOfTransactions > 2
  AND transactionNumber = 2
  AND TIMESTAMP_DIFF(CAST(date AS TIMESTAMP), previousTransactionDate, DAY) <= 7
