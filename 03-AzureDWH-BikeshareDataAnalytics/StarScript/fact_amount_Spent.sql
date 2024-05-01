
-- Fact Table

CREATE TABLE fact_amount_Spent (
    payment_id INT,
    rider_id INT,
	payment_date DATE,
    amount MONEY
);

-- Load data into fact_amount_Spent
INSERT INTO fact_amount_Spent (payment_id, rider_id, payment_date, amount)
SELECT DISTINCT
	p.payment_id,
	r.rider_id,
    p.date,
    p.amount
FROM dbo.stage_payment p 
JOIN dbo.rider_dimension r
on p.rider_id = r.rider_id;