SELECT      eap.date,
            SUM(eap.payableamount) AS totalAmount
FROM        "data_lake_db"."silver_earnings_by_performer" eap
INNER JOIN  "data_lake_db"."bronze_users" us
    ON      (eap.emailaddress = us.streamateuser OR eap.emailaddress = us.jasminuser)
WHERE       CAST(eap.date AS DATE) BETWEEN DATE('2024-01-01') AND DATE('2024-12-31') AND
            us.city = 'Medellín' AND
            us.office = 'L1' AND
            us.artisticname = 'Zoe Do Santos'
GROUP BY    eap.date
ORDER BY    eap.date;