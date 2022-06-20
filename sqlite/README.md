# Linear reggression model #
### SQLite3 ###

This is a simple application that processes information from temperature sensors 
and calculates next expected temperature using linear regression.

The linear regression formula is complicated enough so that it is necessary to write 
a nested query. This is to demonstrate that SQL is not really capable of handling such formulas - 
the overhead related to subsequent queries (nested or chained as view) makes formulas difficult to read.

### Database structure ###

The application consists of two tables:
1. **base** which contains full period data for a sensor in a given period, together with intermediate factors required to calculate linear regression
2. **input** which contains new data for a current period

The data flow is made of the following stages:

1. determine current period from input data (assuming that this is constant for the whole table): **input_period** view
2. get previous period base: **prev_base** view 
3. join previous period base with current input: **prev_base_with_input** view
4. calculate current period values: **prev_base_with_input_updated** view
5. insert results into base and clear the input table: **lin_reg_process_input.sql** script

### Running the application ###

The script to blank create database is **lin_reg_create.sql**. You may want to process all periods at once using **lin_reg_process_all.sql**

Windows users may find useful simple batch files for creating, processing and viewing the database: 
**lin_reg_create_db.bat**, **lin_reg_process.bat**, **browse.bat**. 

All you need to do is make sure **sqlite3.exe** (or other appropriate executable for other OS) is accessible from the script directory.
