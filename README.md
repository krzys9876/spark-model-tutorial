# 3 ways to implement calculation using Spark #

Sometimes when it comes to databases and/or structured files it is necessary not only to do simple transformations  
(e.g. change data format, decode, enrich etc.) but also to calculate complex formulas in order to 
implement some business algorithms.

In this simple project I would like to demonstrate how such a task could be approached in Apache Spark.

I often refer to applications that realise business algorithms as "models" and I use this term throughout the tutorial.  

## The model ##

Suppose we have several temperature sensors and we track them every given time (the period).
Periods are numbered in sequence from 1.

It is necessary not only to just record temperatures but also to be able to calculate the next
expected value. I use linear regression as good enough forecasting algorithm.

For efficiency reasons we should not scan the whole file to calculate regression factors, 
therefore all intermediate formulas are stored together with all other data in a single record per sensor and period.

### Formulas ###

For any given **x** and **y**, **x~next~** and linear regression factors **a** and **b** the **y~next~** can be calculated as:

y=ax+b

or rather:

y~next~ = x~next~ * a + b

**x** is a period, **y** is a temperature. We know next period by adding 1 to the current number.
The complicated part is **a** and **b**. They use 5 intermediate formulas:

**period_count** - equivalent of SQL count(period) assuming that all values are distinct for a sensor
**sum_period_temp** - a sum of multiplies of period and temperature, i.e. sum(period * temperature)
**sum_period** - a sum of all period numbers, i.e. sum(period)
**sum_temp** - a sum of all temperatures, i.e. sum(temp)
**sum_period_sqr** - a sum of squares of period, i.e sum(period*period)

Note that all of them are additive, so we can calculate them incrementally. 
Given these formulas we get:

    a = (period_count * sum_period_temp - sum_period * sum_temp) / 
        (period_count * sum_period_sqr - sum_period * sum_period)

    b = (sum_temp * sum_period_sqr - sum_period * sum_period_temp) / 
        (period_count * sum_period_sqr - sum_period * sum_period)

### SQLite prototype ###

I started with a prototype written in pure SQL using SQLite. It is described in separate directory, 
just note how the formulas are written in SQL code:

    -- LAYER 3 --
    select sensor,period,temp,temp_extrapl,next_period,next_period*lin_reg_a+lin_reg_b as next_temp_extrapl,
    lin_reg_a,lin_reg_b,
    period_count,sum_period_temp,sum_period,sum_temp,sum_period_sqr
    -- LAYER 2 --
    from (
    select sensor,period,temp,temp_extrapl,next_period,
    (period_count * sum_period_temp-sum_period * sum_temp)/(period_count * sum_period_sqr-sum_period * sum_period) as lin_reg_a,
    (sum_temp * sum_period_sqr-sum_period * sum_period_temp)/(period_count * sum_period_sqr-sum_period * sum_period) as lin_reg_b,
    period_count,sum_period_temp,sum_period,sum_temp,sum_period_sqr
    -- LAYER 1 --
    from (
    select input_sensor as sensor,input_period as period,input_temp as temp,next_temp_extrapl as temp_extrapl,input_period+1 as next_period,
    coalesce(period_count,0)+1 as period_count,
    coalesce(sum_period_temp,0)+input_period*input_temp as sum_period_temp,
    coalesce(sum_period,0)+input_period as sum_period,
    coalesce(sum_temp,0)+input_temp as sum_temp,
    coalesce(sum_period_sqr,0)+input_period*input_period as sum_period_sqr
    from prev_base_with_input));

As you can see in order to calculate next temperature (next_temp_extrapl) we need to write 3-layer query:
1. first the intermediate formulas
2. then the linear regression factors
3. and finally the **y=ax+b** formula

This isn't the most readable code, is it?

## Spark take 1 - SQL ##

When browsing for Spark tutorials you probably came across Spark SQL - a way to interact with Spark using commonly known SQL syntax.
Unfortunately this is still "only" SQL and its readability of layered formulas is not great.

Spark SQL has also another downside - the code is just a string and as such cannot be verified at compile time. 
This may not be a problem for simple queries but development of calculation model using plain text is neither secure nor comfortable.

      def processSparkSQL(baseFile:String, inputFile:String)(implicit spark:SparkSession):DataFrame= {
        val joined=getJoinedInputWithBase(baseFile,inputFile)
    
        joined.createOrReplaceTempView("joined")
    
        val preOutput=spark.sql(
          """
            |select
            | sensor,
            | input_period as period,
            | input_temp as temp,
            | next_temp_extrapl as temp_extrapl,
            | input_period+1 as next_period,
            | period_count+1 as period_count,
            | sum_period_temp+input_period*input_temp as sum_period_temp,
            | sum_period+input_period as sum_period,
            | sum_temp+input_temp as sum_temp,
            | sum_period_sqr+input_period*input_period as sum_period_sqr
            | from joined
            | """.stripMargin)
    
        preOutput.createOrReplaceTempView("pre_output")
    
        val preOutput2=spark.sql(
          """
            |select
            | sensor,period,temp,temp_extrapl,next_period,period_count,sum_period_temp,sum_period,sum_temp,sum_period_sqr,
            | (period_count * sum_period_temp - sum_period * sum_temp)/
            | (period_count * sum_period_sqr - sum_period * sum_period) as lin_reg_a,
            | (sum_temp * sum_period_sqr - sum_period * sum_period_temp)/
            | (period_count * sum_period_sqr - sum_period * sum_period) as lin_reg_b
            | from pre_output
          """.stripMargin
        )
    
        preOutput2.createOrReplaceTempView("pre_output2")
    
        val output=spark.sql(
          """
            |select
            | sensor,period,temp,temp_extrapl,next_period,
            | next_period * lin_reg_a + lin_reg_b as next_temp_extrapl,
            | lin_reg_a,lin_reg_b,
            | period_count,sum_period_temp,sum_period,sum_temp,sum_period_sqr
            | from pre_output2
          """.stripMargin
        )
    
        output.show()
        output
    
    }

That looks even worse than in pure SQL...

## Spark take 2 - API functions ##

You don't have to use SQL, spark provides a great API for data manipulation. Consider this:

    def processSparkFunctions(baseFile:String, inputFile:String)(implicit spark:SparkSession):DataFrame= {
    val joined=getJoinedInputWithBase(baseFile,inputFile)
    
        val output=
          joined
            .withColumn("period",col("input_period"))
            .withColumn("temp",col("input_temp"))
            .withColumn("temp_extrapl",col("next_temp_extrapl"))
            .withColumn("next_period",col("input_period")+1)
            .withColumn("period_count",coalesce(col("period_count"),lit(0))+1)
            .withColumn("sum_period_temp",coalesce(col("sum_period_temp"),lit(0))+(col("input_period")*col("input_temp")))
            .withColumn("sum_period",coalesce(col("sum_period"),lit(0))+col("input_period"))
            .withColumn("sum_temp",coalesce(col("sum_temp"),lit(0))+col("input_temp"))
            .withColumn("sum_period_sqr",coalesce(col("sum_period_sqr"),lit(0))+col("input_period")*col("input_period"))
            .withColumn("lin_reg_a",
              (col("period_count") * col("sum_period_temp") - col("sum_period") * col("sum_temp"))/
                (col("period_count") * col("sum_period_sqr") - col("sum_period") * col("sum_period")))
            .withColumn("lin_reg_b",
              (col("sum_temp") * col("sum_period_sqr") - col("sum_period") * col("sum_period_temp"))/
                (col("period_count") * col("sum_period_sqr") - col("sum_period") * col("sum_period")))
            .withColumn("next_temp_extrapl",col("lin_reg_a") * col("next_period") + col("lin_reg_b"))
            .drop("input_period","input_temp")
    
        output.show()
        output
    }

It's shorter, all we do here is calculation of one column at a time. This is much better to me. It is clearer and compile-time verifiable.
Of course - the compiler will not correct typos in column names, but they can easily be replaced with constants.

One could argue that chaining *withColumn* functions degrades performance. This should be checked every time we perceive speed as a decisive factor.
Quite often this will not be a case and if only the performance is satisfactory readability should be regarded as critical.

But this still isn't perfect (if anything is...)

Take a look at testability. In order to calculate anything through Spark you must instantiate a SparkSession object, 
run the process and eventually close the session. This all takes precious time. One spark-based test can take many seconds, sometimes even a few minutes.

This practically cancels falst TDD flow of Red/Green/Refactor, where we aim at miliseconds or seconds at mosts.

Is there something we can do about it?

## Spark take 3 - integration with native Scala ##