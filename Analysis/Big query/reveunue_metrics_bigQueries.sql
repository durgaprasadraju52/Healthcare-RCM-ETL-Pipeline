-- Query 1: Total Revenue, Total Collected, and Overall Collection Rate
-- This provides a high-level overview of the network's financial health.
SELECT
  SUM(ClaimAmount) AS TotalBilledAmount,
  SUM(PaidAmount) AS TotalCollectedAmount,
  (SUM(PaidAmount) / SUM(ClaimAmount)) * 100 AS OverallCollectionRatePercent
FROM
  `healthcare-rcm-project.healthcare_rcm.fact_claims`;

-- Query 2: Revenue and Collection Performance by Hospital
-- This breaks down financial performance to see which hospital is doing better.
SELECT
  p.source_hospital,
  SUM(fc.ClaimAmount) AS TotalBilledAmount,
  SUM(fc.PaidAmount) AS TotalCollectedAmount,
  AVG(fc.days_to_payment) AS AverageDaysToPayment
FROM
  `healthcare-rcm-project.healthcare_rcm.fact_claims` AS fc
JOIN
  `healthcare-rcm-project.healthcare_rcm.dim_patients` AS p ON fc.patient_sk = p.patient_sk
GROUP BY
  p.source_hospital
ORDER BY
  TotalBilledAmount DESC;

-- Query 3: Monthly Revenue and Collection Trends
-- This helps identify seasonality or trends in billing and collections over time.
SELECT
  d.year,
  d.month,
  SUM(fc.ClaimAmount) AS MonthlyBilledAmount,
  SUM(fc.PaidAmount) AS MonthlyCollectedAmount
FROM
  `healthcare-rcm-project.healthcare_rcm.fact_claims` AS fc
JOIN
  `healthcare-rcm-project.healthcare_rcm.dim_date` AS d ON fc.date_sk = d.date_sk
GROUP BY
  d.year,
  d.month
ORDER BY
  d.year,
  d.month;