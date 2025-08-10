-- Query 10: Profitability Analysis by Medical Procedure
-- This query aggregates revenue by procedure and calculates key metrics like average revenue per procedure.
SELECT
  dp.ProcedureDescription,
  dp.ProcedureCode,
  COUNT(ft.TransactionID) AS NumberOfTimesPerformed,
  SUM(ft.Amount) AS TotalRevenueGenerated,
  AVG(ft.Amount) AS AverageRevenuePerProcedure,
  SUM(ft.PaidAmount) AS TotalCollected,
  (SUM(ft.PaidAmount) / SUM(ft.Amount)) * 100 as CollectionRatePercent
FROM
  `healthcare-rcm-project.healthcare_rcm.fact_transactions` AS ft
JOIN
  `healthcare-rcm-project.healthcare_rcm.dim_procedures` AS dp
  ON ft.procedure_sk = dp.procedure_sk
GROUP BY
  dp.ProcedureDescription,
  dp.ProcedureCode
ORDER BY
  TotalRevenueGenerated DESC;