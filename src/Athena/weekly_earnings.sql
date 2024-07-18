SELECT  YEAR(CAST(eap.date AS DATE)) AS year,
        WEEK(CAST(eap.date AS DATE)) AS week,
        SUM(eap.payableamount)  AS totalAmount,
        SUM(eap.onlineseconds) AS totalSeconds
FROM        "data_lake_db"."bronze_earnings_by_performer" eap
GROUP BY    YEAR(CAST(eap.date AS DATE)), WEEK(CAST(eap.date AS DATE));