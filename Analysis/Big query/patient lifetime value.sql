-- Query 9: Top 20 Most Valuable Patients by Total Billed Amount
-- This query ranks individual patients by the total amount billed for their care across all their encounters.
SELECT
  p.patient_sk,
  p.FirstName,
  p.LastName,
  p.source_hospital,
  SUM(fc.ClaimAmount) AS TotalBilledToPatient,
  COUNT(DISTINCT fc.ClaimID) AS NumberOfClaims
FROM
  `healthcare-rcm-project.healthcare_rcm.fact_claims` AS fc
JOIN
  `healthcare-rcm-project.healthcare_rcm.dim_patients` AS p
  ON fc.patient_sk = p.patient_sk
GROUP BY
  p.patient_sk,
  p.FirstName,
  p.LastName,
  p.source_hospital
ORDER BY
  TotalBilledToPatient DESC
LIMIT 20;