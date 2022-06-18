CREATE TABLE base (sensor int8, period int8, temp real, temp_extrapl real, next_period int8, next_temp_extrapl real, lin_reg_a real, lin_reg_b real);
CREATE TABLE input (sensor int8, period int8, temp real);

CREATE VIEW input_period as 
select distinct period, case when period>1 then period-1 end as prev_period 
from input;

CREATE VIEW prev_base as 
select * from base 
where period=(select prev_period from input_period);

CREATE VIEW prev_base_with_input as 
select b.sensor,b.period,b.temp,b.temp_extrapl,b.next_period,b.next_temp_extrapl,b.lin_reg_a,b.lin_reg_b,
i.sensor as input_sensor,i.period as input_period,i.temp as input_temp
from prev_base b 
join input i on (b.sensor=i.sensor)
union
select b.sensor,b.period,b.temp,b.temp_extrapl,b.next_period,b.next_temp_extrapl,b.lin_reg_a,b.lin_reg_b,
i.sensor as input_sensor,i.period as input_period,i.temp as input_temp
from input i
left join prev_base b on (b.sensor=i.sensor);

CREATE VIEW lin_reg as 
with lin_reg_formulas as
(select sensor,max(period) as last_period,
count(period) as cnt,
count(period)*sum(period*temp) as f1,
sum(period)*sum(temp) as f2,
count(period)*sum(period*period) as f3,
sum(period)*sum(period) as f4,
sum(temp)*sum(period*period) as f5,
sum(period)*sum(period*temp) as f6,
(select coalesce(max(b2.period),0)+1 from base b2 where b2.sensor=b.sensor) as next_period
from base b
group by sensor)
select sensor,last_period,next_period,next_period*lin_reg_a+lin_reg_b as next_temp,lin_reg_a,lin_reg_b
from (
select sensor,last_period,
(f1-f2)/(f3-f4) as lin_reg_a,
(f5-f6)/(f3-f4) as lin_reg_b,
next_period
from lin_reg_formulas f);