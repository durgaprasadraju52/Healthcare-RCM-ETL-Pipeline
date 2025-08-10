-- Query 5: Patient Volume and Demographics by Hospital
-- Understands the patient population at each facility.
SELECT
  p.source_hospital,
  p.Gender,
  COUNT(DISTINCT p.patient_sk) AS NumberOfPatients,
  AVG(p.age) AS AveragePatientAge
FROM
  `healthcare-rcm-project.healthcare_rcm.dim_patients` AS p
WHERE p.is_current = TRUE -- Analyze only the current demographic data
GROUP BY
  p.source_hospital,
  p.Gender
ORDER BY
  p.source_hospital,
  NumberOfPatients DESC;

-- Query 6: Insurance Mix Analysis
-- Shows the distribution of patients across different payer types.
SELECT
  fc.PayorType,
  COUNT(DISTINCT fc.patient_sk) AS NumberOfUniquePatients
FROM
  `healthcare-rcm-project.healthcare_rcm.fact_claims` fc
GROUP BY
  fc.PayorType
ORDER BY
  NumberOfUniquePatients DESC;