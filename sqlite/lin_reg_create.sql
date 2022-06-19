CREATE TABLE base 
(sensor int8, period int8, temp real, temp_extrapl real, next_period int8, next_temp_extrapl real, 
lin_reg_a real, lin_reg_b real, period_count long, sum_period_temp real, sum_period real, sum_temp real, sum_period_sqr real);

CREATE TABLE input (sensor int8, period int8, temp real);

CREATE VIEW input_period as 
select distinct period, case when period>1 then period-1 end as prev_period 
from input;

CREATE VIEW prev_base as 
select * from base 
where period=(select prev_period from input_period);

CREATE VIEW prev_base_with_input as 
select b.sensor,b.period,b.temp,b.temp_extrapl,b.next_period,b.next_temp_extrapl,
b.lin_reg_a,b.lin_reg_b,b.period_count,b.sum_period_temp,b.sum_period,b.sum_temp,b.sum_period_sqr,
i.sensor as input_sensor,i.period as input_period,i.temp as input_temp
from prev_base b 
join input i on (b.sensor=i.sensor)
union
select b.sensor,b.period,b.temp,b.temp_extrapl,b.next_period,b.next_temp_extrapl,
b.lin_reg_a,b.lin_reg_b,b.period_count,b.sum_period_temp,b.sum_period,b.sum_temp,b.sum_period_sqr,
i.sensor as input_sensor,i.period as input_period,i.temp as input_temp
from input i
left join prev_base b on (b.sensor=i.sensor);

CREATE VIEW prev_base_with_input_updated
as
select sensor,period,temp,temp_extrapl,next_period,next_period*lin_reg_a+lin_reg_b as next_temp_extrapl,
lin_reg_a,lin_reg_b,
period_count,sum_period_temp,sum_period,sum_temp,sum_period_sqr
from (
select sensor,period,temp,temp_extrapl,next_period,
(period_count * sum_period_temp-sum_period * sum_temp)/(period_count * sum_period_sqr-sum_period * sum_period) as lin_reg_a,
(sum_temp * sum_period_sqr-sum_period * sum_period_temp)/(period_count * sum_period_sqr-sum_period * sum_period) as lin_reg_b,
period_count,sum_period_temp,sum_period,sum_temp,sum_period_sqr
from (
select input_sensor as sensor,input_period as period,input_temp as temp,next_temp_extrapl as temp_extrapl,input_period+1 as next_period,
coalesce(period_count,0)+1 as period_count,
coalesce(sum_period_temp,0)+input_period*input_temp as sum_period_temp,
coalesce(sum_period,0)+input_period as sum_period,
coalesce(sum_temp,0)+input_temp as sum_temp,
coalesce(sum_period_sqr,0)+input_period*input_period as sum_period_sqr
from prev_base_with_input));
