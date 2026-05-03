-- Date dimension spanning 2020-01-01 to 2030-12-31
with date_spine as (
    select
        DATEADD(day, SEQ4(), '2020-01-01'::DATE) as date_day
    from TABLE(GENERATOR(ROWCOUNT => 4018))  -- ~11 years
)

select
    date_day                                            as date_id,
    date_day,
    DATE_PART('year', date_day)::NUMBER                 as year,
    DATE_PART('quarter', date_day)::NUMBER              as quarter,
    DATE_PART('month', date_day)::NUMBER                as month,
    MONTHNAME(date_day)                                 as month_name,
    DATE_PART('week', date_day)::NUMBER                 as week_of_year,
    DATE_PART('dayofyear', date_day)::NUMBER            as day_of_year,
    DATE_PART('dayofweek', date_day)::NUMBER            as day_of_week,
    DAYNAME(date_day)                                   as day_name,
    DATE_TRUNC('week', date_day)::DATE                  as week_start_date,
    DATE_TRUNC('month', date_day)::DATE                 as month_start_date,
    DATE_TRUNC('quarter', date_day)::DATE               as quarter_start_date,
    DATE_TRUNC('year', date_day)::DATE                  as year_start_date,
    CASE DATE_PART('dayofweek', date_day)
        WHEN 0 THEN FALSE
        WHEN 6 THEN FALSE
        ELSE TRUE
    END                                                 as is_weekday
from date_spine
