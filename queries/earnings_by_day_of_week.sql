SELECT  CASE 
            WHEN day_of_week(CAST(eap."date" AS DATE)) = 1 THEN 'Lun'
            WHEN day_of_week(CAST(eap."date" AS DATE)) = 2 THEN 'Mar'
            WHEN day_of_week(CAST(eap."date" AS DATE)) = 3 THEN 'Mié'
            WHEN day_of_week(CAST(eap."date" AS DATE)) = 4 THEN 'Jue'
            WHEN day_of_week(CAST(eap."date" AS DATE)) = 5 THEN 'Vie'
            WHEN day_of_week(CAST(eap."date" AS DATE)) = 6 THEN 'Sáb'
            WHEN day_of_week(CAST(eap."date" AS DATE)) = 7 THEN 'Dom'
        END AS DOW,
        ROUND(SUM(eap.payableamount), 2) AS TOTAL,
        ROUND((SUM(eap.payableamount) / (SELECT SUM(eap_inner.payableamount) 
                                         FROM "data_lake_db"."silver_earnings_by_performer" eap_inner
                                         INNER JOIN "data_lake_db"."bronze_users" us_inner 
                                            ON (eap_inner.emailaddress = us_inner.streamateuser OR eap_inner.emailaddress = us_inner.jasminuser)
                                         WHERE CAST(eap_inner."date" AS DATE) BETWEEN DATE('2024-09-01') AND DATE('2024-09-30')
                                        ) * 100, 2) AS percentage
FROM        "data_lake_db"."silver_earnings_by_performer" eap
INNER JOIN  "data_lake_db"."bronze_users" us
    ON (eap.emailaddress = us.streamateuser OR eap.emailaddress = us.jasminuser)
WHERE   CAST(eap."date" AS DATE) BETWEEN DATE('2024-09-01') AND DATE('2024-09-30')
GROUP BY day_of_week(CAST(eap."date" AS DATE))
ORDER BY day_of_week(CAST(eap."date" AS DATE)) ASC;