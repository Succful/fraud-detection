with custAVG as (
    SELECT client_id , 
    AVG(CAST(REPLACE(amount, '$', '') AS REAL)) as AVGTRANS
    from transactions_data
    where merchant_state is not null 
    and merchant_city <> 'ONLINE'
    GROUP by client_id
),

CTE as (
    select client_id, count(*) as TRANSACTIONDAY,
    count(DISTINCT merchant_state) as uniq_trans,
    AVG(CAST(replace(amount, '$', '') AS REAL)) as avgperday
    from transactions_data
    where merchant_state is not null 
    and merchant_city <> 'ONLINE'	
    group by client_id, date(date)
),

task01 as (
    -- Клиенты с аномальным числом транзакций за день
    select DISTINCT client_id, TRANSACTIONDAY, uniq_trans, 
    round(avgperday / avgtrans, 2) as ratio_risk,
    case 
        when TRANSACTIONDAY > 10 THEN 'Критический'
        when TRANSACTIONDAY > 5 Then 'Высокий'
        ELSE 'Средний'
    end as Risk
    from CTE
    join custAVG using(client_id)
    where uniq_trans > 1 
    and AVGTRANS * 2 < avgperday
    and TRANSACTIONDAY > 10
),

AVGover2 as (
    -- Средний чек по всем транзакциям
    select round(AVG(CAST(REPLACE(amount, '$', '') AS REAL)), 2) as avgover
    FROM transactions_data 
), 

mercherror as (
    -- Мерчанты где более 20% транзакций с ошибками
    select merchant_id, 
    SUM(CASE WHEN errors IS NOT NULL THEN 1 ELSE 0 END) as error_count,
    count(*) as total, 
    round(SUM(CASE WHEN errors IS NOT NULL THEN 1 ELSE 0 END) / count(*) * 100, 2) as totalerror,
    round(avg(CAST(REPLACE(amount, '$', '') AS REAL)), 2) as avg_merchant
    from transactions_data
    GROUP by merchant_id
    HAVING totalerror > 50
),

task02 as (
    -- Клиенты которые платили у подозрительных мерчантов
    select DISTINCT t.client_id, 
    t.merchant_id, 
    m.totalerror,
    m.avg_merchant,
    avgover,
    ROUND(m.avg_merchant - avgover, 2) AS diff_from_avg
    from transactions_data t
    JOIN mercherror m ON t.merchant_id = m.merchant_id
    CROSS JOIN AVGover2
    where m.totalerror > 20
), 

night as ( 
    -- Средний чек ночью для каждого клиента
    SELECT client_id, 
    round(AVG(CAST(REPLACE(amount, '$', '') AS REAL)), 2) as avgn
    from transactions_data
    where strftime('%H', date) BETWEEN '00' and '05'
    group by client_id
),

days as ( 
    -- Средний чек днём для каждого клиента
    SELECT client_id, 
    round(AVG(CAST(REPLACE(amount, '$', '') AS REAL)), 2) as avgd
    from transactions_data
    where strftime('%H', date) BETWEEN '06' and '23'
    group by client_id
),

task3 as (
    -- Клиенты у которых ночной чек выше дневного в 1.5+ раза
    SELECT client_id, avgn, avgd, round(avgn / avgd, 2) as ratio,
    case 
        WHEN round(avgn / avgd, 2) > 4 then 'Критичность'
        when round(avgn / avgd, 2) > 2 then 'Опасность'
        else 'Внимание'
    end as risk
    from night
    join days using(client_id)
    where avgn / avgd > 3
), 

dates as ( 
    -- Добавляем время следующей транзакции для каждого клиента
    select client_id, date, 
    lead(date) over(PARTITION by client_id order by date) as datee,
    amount,  
    lead(amount) over(PARTITION by client_id order by date) as amoount
    from transactions_data 
),

over1 as ( 
    -- Оставляем только пары транзакций с интервалом менее 5 минут
    select client_id, date, datee,  
    CEILING((julianday(datee) - julianday(date)) * 24 * 60) AS diff_seconds, 
    amount, amoount
    from dates 
    where CEILING((julianday(datee) - julianday(date)) * 24 * 60) < 2  
    and CEILING((julianday(datee) - julianday(date)) * 24 * 60) <> 0 
),

AVGover1 as (
    select round(AVG(CAST(REPLACE(amount, '$', '') AS REAL)), 2) as avgover
    FROM transactions_data 
),

task4 as (
    -- Клиенты с быстрыми транзакциями выше среднего
    select client_id, date, amount, avgover, diff_seconds, amoount, datee
    from over1, AVGover1
    where diff_seconds < 5 
    and diff_seconds <> 0 
    and CAST(REPLACE(amount, '$', '') AS REAL) > avgover * 5
    and CAST(REPLACE(amoount, '$', '') AS REAL) > avgover * 5
    and amoount not like '%-%'
)



SELECT 
    t.client_id,
    MAX(CASE WHEN t1.client_id IS NOT NULL THEN 1 ELSE 0 END) AS flag_velocity,
    MAX(CASE WHEN t2.client_id IS NOT NULL THEN 1 ELSE 0 END) AS flag_merch,
    MAX(CASE WHEN t3.client_id IS NOT NULL THEN 1 ELSE 0 END) AS flag_night,
    MAX(CASE WHEN t4.client_id IS NOT NULL THEN 1 ELSE 0 END) AS flag_fast_tx,

    -- Итоговый скор
    (MAX(CASE WHEN t1.client_id IS NOT NULL THEN 1 ELSE 0 END) +
     MAX(CASE WHEN t2.client_id IS NOT NULL THEN 1 ELSE 0 END) +
     MAX(CASE WHEN t3.client_id IS NOT NULL THEN 1 ELSE 0 END) +
     MAX(CASE WHEN t4.client_id IS NOT NULL THEN 1 ELSE 0 END)) AS risk_score,

    CASE 
        WHEN (MAX(CASE WHEN t1.client_id IS NOT NULL THEN 1 ELSE 0 END) +
              MAX(CASE WHEN t2.client_id IS NOT NULL THEN 1 ELSE 0 END) +
              MAX(CASE WHEN t3.client_id IS NOT NULL THEN 1 ELSE 0 END) +
              MAX(CASE WHEN t4.client_id IS NOT NULL THEN 1 ELSE 0 END)) >= 3 THEN 'Критический'
        WHEN (MAX(CASE WHEN t1.client_id IS NOT NULL THEN 1 ELSE 0 END) +
              MAX(CASE WHEN t2.client_id IS NOT NULL THEN 1 ELSE 0 END) +
              MAX(CASE WHEN t3.client_id IS NOT NULL THEN 1 ELSE 0 END) +
              MAX(CASE WHEN t4.client_id IS NOT NULL THEN 1 ELSE 0 END)) = 2 THEN 'Высокий'
        ELSE 'Средний'
    END AS risk_level

FROM (SELECT DISTINCT client_id FROM transactions_data) t
LEFT JOIN task01 t1 ON t.client_id = t1.client_id
LEFT JOIN task02 t2 ON t.client_id = t2.client_id
LEFT JOIN task3 t3 ON t.client_id = t3.client_id
LEFT JOIN task4 t4 ON t.client_id = t4.client_id
GROUP BY t.client_id 

HAVING (MAX(CASE WHEN t1.client_id IS NOT NULL THEN 1 ELSE 0 END) +
       MAX(CASE WHEN t2.client_id IS NOT NULL THEN 1 ELSE 0 END) +
       MAX(CASE WHEN t3.client_id IS NOT NULL THEN 1 ELSE 0 END) +
       MAX(CASE WHEN t4.client_id IS NOT NULL THEN 1 ELSE 0 END)) > 0

ORDER BY 6 DESC
