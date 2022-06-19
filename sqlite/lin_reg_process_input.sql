INSERT INTO base (sensor,period,temp,temp_extrapl,next_period,next_temp_extrapl,lin_reg_a,lin_reg_b,period_count,sum_period_temp,sum_period,sum_temp,sum_period_sqr) 
select sensor,period,temp,temp_extrapl,next_period,next_temp_extrapl,lin_reg_a,lin_reg_b,period_count,sum_period_temp,sum_period,sum_temp,sum_period_sqr from prev_base_with_input_updated;

delete from input;
