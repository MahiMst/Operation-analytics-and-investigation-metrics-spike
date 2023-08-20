create database project3;
show database;
use project3

# table- users
user_id	created_at	company_id	language	activated_at	state

create table users (
user_id  int,
created_at varchar(100),
company_id int,
language varchar(50),
activated_at varchar(100),
state varchar(50));

show variables like 'secure_file_priv';

load data infile "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/users.csv"
into table users
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows;

select * from users;
alter table users add column temp_created_at datetime;
UPDATE users SET temp_created_at = STR_TO_DATE(created_at, '%d-%m-%Y %H:%i');
alter table users drop column created_at;
alter table users change column temp_created_at created_at datetime;










# table-2 events 
create table events(
user_id int,
occurred_at varchar(100),
event_type varchar(50),
EVENT_NAME varchar(100),
location varchar(50),
device varchar(50),
user_type int
);


load data infile "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/events.csv"
into table events
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows;
 
 desc events;
 
 
SELECT * FROM events;

ALTER TABLE events add column temp_occurred_at datetime;

UPDATE events SET temp_occurred_at = STR_TO_DATE(occurred_at, '%d-%m-%Y %H:%i');

alter table events drop column occurred_at;

alter table events change column temp_occurred_at occurred_at datetime;


# table 3 emailevents


create table emailEvents(
user_id int,
occurred_at varchar(100),
action varchar(100),
user_type int
);

load data infile "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/email_events.csv"
into table emailEvents
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows;

select * from emailEvents;

ALTER TABLE emailevents add column temp_occurred_at datetime;

UPDATE emailevents SET temp_occurred_at = STR_TO_DATE(occurred_at, '%d-%m-%Y %H:%i');

alter table emailevents drop column occurred_at;

alter table emailevents change column temp_occurred_at occurred_at datetime;




select * from events;

-- 1 Weekly User Engagement:Objective: Measure the activeness of users on a weekly basis.Your Task: Write an SQL query to calculate the weekly user engagement.

SELECT
    DATE_FORMAT(occurred_at, '%W') AS week_start,
    COUNT(DISTINCT user_id) AS weekly_active_users
FROM
    events
GROUP BY
    week_start;



-- 2 User Growth Analysis

SELECT
    DATE_FORMAT(occurred_at, '%Y-%m') AS signup_month,
    COUNT(DISTINCT user_id) AS new_users
FROM
    events
GROUP BY
    signup_month;


-- 3 Weekly Retention Analysis
select * from events;


SELECT
    MONTH(this_month.occurred_at) AS month,
    COUNT(DISTINCT last_month.user_id) AS retained_users
FROM
    events this_month
LEFT JOIN
    events last_month ON this_month.user_id = last_month.user_id
                      AND DATEDIFF(this_month.occurred_at, last_month.occurred_at) = 1
GROUP BY
    MONTH(this_month.occurred_at);



--  4 Weekly Engagement Per Device

SELECT
    DATE_FORMAT(occurred_at, '%Y-%m-%d') AS week_start,
    events.device,
    COUNT(DISTINCT events.user_id) AS weekly_active_users
FROM
    events
GROUP BY
    week_start, events.device;
    
    
    -- # table 3 enet email
    
   -- user_id	occurred_at	action	user_type




 
-- 5 Email Engagement Analysis by mahendra singh
    

select * from emailevents;


SELECT
    DATE(occurred_at) AS email_date,
    COUNT(DISTINCT user_id) AS total_users,
    COUNT(DISTINCT CASE WHEN action = 'email_open' THEN user_id END) AS opened_emails,
    COUNT(DISTINCT CASE WHEN action = 'email_clickthrough' THEN user_id END) AS clicked_links,
    COUNT(DISTINCT CASE WHEN action = 'sent_weekly_digest' THEN user_id END) AS weekly_digest,
    COUNT(DISTINCT CASE WHEN action = 'sent_reengagement_email' THEN user_id END) AS sent_reengagment
FROM
    emailevents
WHERE
    action IN ('email_open', 'email_clickthrough', 'sent_weekly_digest', 'sent_reengagement_email') -- Filter relevant actions
GROUP BY
    email_date
ORDER BY
    email_date;




-- precticing-------------------------------------------------------------------------------------------------------------------
-- tables of it email_open email_clickthrough sent_weekly_digest sent_reengagement_email

-- precticing email engagements per month 


SELECT
    month(occurred_at) AS month,
    COUNT(DISTINCT user_id) AS total_users,
    COUNT(DISTINCT CASE WHEN action = 'email_open' THEN user_id END) AS opened_emails,
    COUNT(DISTINCT CASE WHEN action = 'email_clickthrough' THEN user_id END) AS clicked_links,
    COUNT(DISTINCT CASE WHEN action = 'sent_weekly_digest' THEN user_id END) AS weekly_digest,
    COUNT(DISTINCT CASE WHEN action = 'sent_reengagement_email' THEN user_id END) AS sent_reengagment
FROM
    emailevents
WHERE
    action IN ('email_open', 'email_clickthrough', 'sent_weekly_digest', 'sent_reengagement_email') -- Filter relevant actions
GROUP BY
    month
ORDER BY
    month;




-- user_id	occurred_at	event_type	event_name	location	device	user_type



    
    
    SELECT
    MONTH(this_month.occurred_at) AS month,
    COUNT(DISTINCT last_month.user_id) AS retained_users
FROM
    events this_month
LEFT JOIN
    events last_month ON this_month.user_id = last_month.user_id
                      AND DATEDIFF(this_month.occurred_at, last_month.occurred_at) = 1
GROUP BY
    MONTH(this_month.occurred_at);

    
    
    
-- prectining 


select * from events;
select * from events;


SELECT
month(this_month.occurred_at) as month,
    COUNT(DISTINCT last_month.user_id) AS retained_users
from
     events this_month
left join
    events occured_at last_month on occured_at this_month
    on this_month.user_id=last_month,user_id and datediff(last_month.occured_at,this_month.occured_at)=1
    group by month(this_month.occurred_at)