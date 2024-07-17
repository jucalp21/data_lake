# AWS Athena Queries.

Consulta por Dia

```sql
    SELECT nickname, date, total_seconds
    FROM "data_lake_db"."bronze_earnings_by_performer"
    WHERE date = DATE '2024-07-17';
```

Consulta por semana

```sql
    SELECT nickname, date, total_seconds
    FROM "data_lake_db"."bronze_earnings_by_performer"
    WHERE WEEK(date) = WEEK(DATE '2024-07-17') AND YEAR(date) = YEAR(DATE '2024-07-17');
```

Consulta por mes

```sql
    SELECT nickname, date, total_seconds
    FROM "data_lake_db"."bronze_earnings_by_performer"
    WHERE MONTH(date) = 7 AND YEAR(date) = 2024;
```

Consulta por año

```sql
    SELECT nickname, date, total_seconds
    FROM "data_lake_db"."bronze_earnings_by_performer"
    WHERE YEAR(date) = 2024;
```

## Agrupación y agregación por periodos

Agrupación diaria

```sql
    SELECT nickname, date, SUM(total_seconds) as total_seconds
    FROM "data_lake_db"."bronze_earnings_by_performer"
    GROUP BY nickname, date;
```

Agrupación semanal

```sql
    SELECT nickname, YEAR(date) as year, WEEK(date) as week, SUM(total_seconds) as total_seconds
    FROM "data_lake_db"."bronze_earnings_by_performer"
    GROUP BY nickname, YEAR(date), WEEK(date);
```

Agrupación mensual

```sql
    SELECT nickname, YEAR(date) as year, MONTH(date) as month, SUM(total_seconds) as total_seconds
    FROM "data_lake_db"."bronze_earnings_by_performer"
    GROUP BY nickname, YEAR(date), MONTH(date);
```

Agrupación anual

```sql
    SELECT nickname, YEAR(date) as year, SUM(total_seconds) as total_seconds
    FROM "data_lake_db"."bronze_earnings_by_performer"
    GROUP BY nickname, YEAR(date);
```
