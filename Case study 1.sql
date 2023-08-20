create database job_data;


DROP DATABASE job_data;



#table users 

-- ds	job_id	actor_id	event	language	time_spent	org

create table usersjob (
ds varchar(100),
job_id int,
actor_id int,
event varchar(50),
language varchar(50),
time_spent int,
org varchar(50));

load data infile "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/job_data.csv"
into table usersjob
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows;


ALTER TABLE usersjob add column temp_ds datetime;

UPDATE usersjob SET temp_ds = STR_TO_DATE(ds, '%d-%m-%y');

alter table emailevents drop column occurred_at;

alter table emailevents change column temp_occurred_at occurred_at datetime;


SELECT ds from usersjob CONVERT(DATETIME,'13/12/2019',103)



select * from usersjob;

-- Calculate the number of jobs reviewed per hour for each day in November 2020.

SELECT
    ds,
    SUM(time_spent) AS total_time_spent_seconds,
    SUM(time_spent) / 3600.0 AS total_time_spent_hours
FROM
    usersjob
GROUP BY
   ds;
   
   
   
   -- precticing


-- throughout

SELECT
    ds,
    AVG(events_per_second) OVER (ORDER BY ds ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_avg_throughput
FROM (
    SELECT
        ds,
        COUNT(*) AS events_per_second
    FROM
        usersjob
    GROUP BY
        ds
) AS daily_events;







-- lanuage

select * from usersjob;


SELECT
    language,
    (SUM(time_spent) / 3600.0 * 100.0 / total_time) AS percentage_share
FROM (
    SELECT
        language,
        SUM(time_spent) AS total_time
    FROM
        usersjob
    WHERE
        time_spent >= 3600 * 24 * 30 -- 30 days in seconds
    GROUP BY
        language
) AS language_time
GROUP BY
    language
ORDER BY
    percentage_share DESC;
    
    
    
    
    -- prepare
    SELECT 
    language,
    COUNT(*) AS events_count,
    (COUNT(*) * 100.0) / (
        SELECT COUNT(*)
        FROM job_data
        WHERE ds >= DATE('now', '-30 days')
    ) AS percentage_share
FROM job_data
WHERE ds >= DATE('now', '-30 days')
GROUP BY language
ORDER BY percentage_share DESC;



SELECT 
    language,
    COUNT(*) AS events_count,
    (COUNT(*) * 100.0) / (
        SELECT COUNT(*)
        FROM job_data
        WHERE DATE(ds) >= DATE('now', '-30 days')
    ) AS percentage_share
FROM job_data
WHERE DATE(ds) >= DATE('now', '-30 days')
GROUP BY language
ORDER BY percentage_share DESC;




-- duplicate

SELECT
    job_id,
    actor_id,
    event,
    language,
    time_spent,
    org,
    ds,
    COUNT(*) AS duplicate_count
FROM
    usersjob
GROUP BY
    job_id, actor_id, event, language, time_spent, org, ds
HAVING
    COUNT(*) > 1;










SELECT
    SUBSTR(ds, 1, 10) AS review_date,
    COUNT(*) AS jobs_reviewed
FROM
    usersjob
WHERE
    event = 'review'
GROUP BY
    review_date
ORDER BY
    review_date;


-- prectice 
SELECT
    language,
    SUM(time_spent) AS total_time_spent,
    (SUM(time_spent) * 100.0 / total_time) AS percentage_share
FROM (
    SELECT
        language,
        SUM(time_spent) AS total_time
    FROM
        usersjob
    WHERE
        ds >= DATE_SUB(NOW(), INTERVAL 30 DAY)
    GROUP BY
        language
) AS language_time
GROUP BY
    language
ORDER BY
    percentage_share DESC;

        

