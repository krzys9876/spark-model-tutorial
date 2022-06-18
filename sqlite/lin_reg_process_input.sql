INSERT INTO base (sensor,period,temp,temp_extrapl) 
select input_sensor,input_period,input_temp,next_temp_extrapl from prev_base_with_input;

UPDATE base set 
next_period=(select next_period from lin_reg l where l.sensor=sensor and l.last_period=period),
next_temp_extrapl=(select next_temp from lin_reg l where l.sensor=sensor and l.last_period=period),
lin_reg_a=(select lin_reg_a from lin_reg l where l.sensor=sensor and l.last_period=period),
lin_reg_b=(select lin_reg_b from lin_reg l where l.sensor=sensor and l.last_period=period) 
where (sensor,period) in (select sensor,last_period from lin_reg);

delete from input;
