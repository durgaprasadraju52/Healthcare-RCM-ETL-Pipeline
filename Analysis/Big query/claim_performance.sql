-- Query 4: Approval and Denial Rates by Payer
-- This is critical for identifying problematic insurance payers.
SELECT
  PayorType,
  COUNT(*) AS TotalClaims,
  SUM(CASE WHEN ClaimStatus = 'Paid' THEN 1 ELSE 0 END) AS ApprovedClaims,
  SUM(CASE WHEN ClaimStatus = 'Denied' THEN 1 ELSE 0 END) AS DeniedClaims,
  (SUM(CASE WHEN ClaimStatus = 'Paid' THEN 1 ELSE 0 END) / COUNT(*)) * 100 AS ApprovalRatePercent,
  (SUM(CASE WHEN ClaimStatus = 'Denied' THEN 1 ELSE 0 END) / COUNT(*)) * 100 AS DenialRatePercent,
  AVG(days_to_payment) AS AverageProcessingTimeInDays
FROM
  `healthcare-rcm-project.healthcare_rcm.fact_claims`
GROUP BY
  PayorType
ORDER BY
  TotalClaims DESC;
  