| Path | Rows | Median ms | Min–max ms | Rows/s |
|---|---:|---:|---:|---:|
| direct_redis_adbc | 100,000 | 792.38 | 789.37–809.98 | 126,202 |
| adbc | 100,000 | 896.32 | 886.10–918.58 | 111,567 |
| jdbc | 100,000 | 940.25 | 936.82–961.03 | 106,355 |
| native_flight_dataset | 100,000 | 37.81 | 28.90–46.56 | 2,644,864 |
| redis_py_scan_hmget_arrow | 100,000 | 3,611.29 | 3,556.56–3,657.23 | 27,691 |

| Query | Rows | Direct Redis ADBC ms | Flight SQL ADBC ms | Flight SQL JDBC ms | Flight ADBC / direct |
|---|---:|---:|---:|---:|---:|
| point lookup by __rowid | 1 | 6.02 | 10.58 | 15.04 | 1.76× |
| indexed filter + top 10 | 10 | 53.02 | 62.07 | 59.18 | 1.17× |
| indexed range COUNT | 1 | 7.60 | 14.15 | 10.36 | 1.86× |
| LIKE prefix COUNT | 1 | 46.87 | 45.91 | 40.28 | 0.98× |
| interval range COUNT | 1 | 4.69 | 12.52 | 6.35 | 2.67× |
| GROUP BY, index reduce | 6 | 266.81 | 258.05 | 254.07 | 0.97× |
| GROUP BY, driver (AVG double) | 6 | 561.15 | 543.15 | 540.04 | 0.97× |
| GROUP BY expression, driver | 5 | 645.74 | 617.75 | 617.34 | 0.96× |
| GROUP BY DATE_TRUNC, driver | 12 | 682.02 | 595.00 | 631.27 | 0.87× |
| non-indexed filter (scan) | 1 | 131.46 | 96.03 | 92.49 | 0.73× |
| fetch 10% of rows | 10,000 | 75.06 | 83.49 | 93.19 | 1.11× |
| full scan, all columns | 100,000 | 792.38 | 896.32 | 940.25 | 1.13× |
| selective join (index lookup) | 8 | 29.58 | 39.22 | 37.02 | 1.33× |
| unfiltered join (hash join) | 6 | 443.12 | 441.31 | 450.45 | 1.00× |
| correlated subquery (50 rows) | 50 | 41.88 | 46.77 | 48.97 | 1.12× |
| IN (SELECT …), half the rows | 1 | 915.42 | 841.07 | 841.15 | 0.92× |
| correlated EXISTS (semi-join) | 1 | 1,111.00 | 1,083.69 | 1,054.75 | 0.98× |
| correlated EXISTS, no inner rows | 1 | 11.88 | 15.68 | 13.72 | 1.32× |
| UNION of two filters | 1,750 | 98.92 | 92.59 | 88.70 | 0.94× |
| view with pushed filter | 1 | 12.78 | 27.03 | 24.48 | 2.11× |
