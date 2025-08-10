-- Query 7: Average Days in Accounts Receivable (A/R)
-- A key metric showing how long it takes to collect payment after providing a service.
SELECT
  AVG(days_to_payment) AS AverageDaysInAR
FROM
  `healthcare-rcm-project.healthcare_rcm.fact_claims`
WHERE
  ClaimStatus = 'Paid';

-- Query 8: Total Write-Off Amounts
-- Calculates the total amount from claims that were denied and never collected.
SELECT
  SUM(ClaimAmount) AS TotalWriteOffAmount
FROM
  `healthcare-rcm-project.healthcare_rcm.fact_claims`
WHERE
  ClaimStatus = 'Denied' AND PaidAmount = 0;