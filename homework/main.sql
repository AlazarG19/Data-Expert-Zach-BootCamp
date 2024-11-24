create type films as (
	film text,
	votes integer,
	rating real,
	filmid text
)

create type quality_class as enum(
	'star','good','average', 'bad'
)

create table actor(
	actor text,
	actorid text,
	films films[],
	quality_class quality_class,
	current_year integer,
	is_active boolean
)

insert into actor
 
with yesterday as (
	select 
		*
	from actor 
	where current_year = 1979
),
today as (
	select 
	actor,
		actorid,
		array_agg(row(film,votes,rating,filmid)::films) as films,
		array_agg(array[rating]) as fd,
		avg(rating) as avg_rating,
--		avg()avg_rating 
--		filmid,
		year
	from actor_films 
	where year = 1980
	group by actor,actorid,year
			order by actor
)
select 
	coalesce(t.actor,y.actor) as actor,
	coalesce(t.actorid,y.actorid) as actorid,
	case when y.films is null then
			t.films
		when t.year is not null then
			y.films || t.films
		else  
			y.films
		end as films,
	case
		when t.avg_rating is not null then
			case 
				when t.avg_rating > 8 then 'star'
				when t.avg_rating > 7 then 'good'
				when t.avg_rating >6 then 'average'
				else 'bad'
			end::quality_class 
		else 
			y.quality_class
			end as quality_class,
	coalesce(t.year,y.current_year+1) as current_year,
	case 
		when t.year is not null then true
		else false
		end as is_active
	from today t full outer join 
	yesterday y on t.actor = y.actor


create table actors_history_scd(
	actor text,
	quality_class quality_class,
	start_date integer,
	end_date integer,
	current_year integer,
	is_active boolean
)

insert into actors_history_scd

with with_previous as (
select
	actor,
	current_year as year,
	quality_class as quality_class,
	lag(quality_class,
	1) over(partition by actor
order by
	current_year) as previous_quality_class,
	is_active as is_active,
	lag(is_active,
	1) over(partition by actor
order by
	current_year) as previous_is_active
from
	actor
),
with_indicator as (
select 
		actor,
		year,
		quality_class,
		previous_quality_class,
		is_active,
		previous_is_active,
		case
		when previous_quality_class <> quality_class then 1
		when is_active <> previous_is_active then 1
		else 0
	end as indicator
from
	with_previous
	order by actor,year
),
with_streak as (
select 
	 	*,
	 	sum(indicator) over (partition by actor order by year) as streak_identifier
from
	with_indicator
)
select
actor,
quality_class,
min(year) as start_date,
max(year) as end_date,
1979 as current_year, 
is_active
from
	with_streak
group by streak_identifier,actor,quality_class,is_active
order by actor,streak_identifier


create type actors_scd_type as (
	quality_class quality_class,
	is_active boolean,
	start_date integer ,
	end_date integer
)

with last_year_scd as(
	select actor,quality_class,is_active,start_date,end_date from actors_history_scd
	where current_year = 1979
	and end_date = 1979
),
history_scd as (
	select actor,quality_class,is_active,start_date,end_date from actors_history_scd 
	where current_year = 1979
	and end_date < 1979
),
this_year_data as (
	select * from actor
	where current_year = 1980
),
unchanged_record_data as (
	select 
	ly.actor as actor,
	ly.quality_class as quality_class,
	ly.is_active,
	ly.start_date as start_date,
	ty.current_year as end_date
	from last_year_scd ly join this_year_data as ty
	on ly.actor=ty.actor 
	where ly.quality_class = ty.quality_class
	and ly.is_active = ty.is_active
),
change_record_data as (
	select 
	ty.actor,
	unnest(
		array[
			row(ly.quality_class,ly.is_active,ly.start_date,ly.end_date)::actors_scd_type,
			row(ty.quality_class,ly.is_active,ty.current_year,ty.current_year)::actors_scd_type
		]) as record
	from this_year_data as ty 
	left join last_year_scd ly 
	on ty.actor = ty.actor
	where ty.quality_class <> ty.quality_class 
	or ly.is_active <> ty.is_active
),
unnested_change_record_data as (
	select 
	actor,
	(record::actors_scd_type).quality_class as quality_class,
	(record::actors_scd_type).is_active as is_active,
	(record::actors_scd_type).start_date as start_date,
	(record::actors_scd_type).end_date as end_date
	from change_record_data
),
new_data as (
	select 
	ty.actor as actor,
	ty.quality_class as quality_class,
	ty.is_active as is_active,
	ty.current_year as start_year,
	ty.current_year as last_year
	from last_year_scd ly left join this_year_data ty 
	on ly.actor = ty.actor 
	where ly.actor is null
)

select * from history_scd
union 
select * from unchanged_record_data
union
select * from unnested_change_record_data
union
select * from new_data




