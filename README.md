# Three ways to implement complex calculations with Apache Spark #

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

For any given $x$ and linear regression factors $a$ and $b$ the $y$ can be calculated as:

$y=ax + b$

$x$ is a period, $y$ is a temperature. We know next period by adding 1 to the current number. The complicated part is $a$ and $b$:

$$ a = { n\displaystyle\sum_{i=1}^{n} x_iy_i - \displaystyle\sum_{i=1}^{n} x_i \displaystyle\sum_{i=1}^{n} y_i  \over n\displaystyle\sum_{i=1}^{n} x_i^2 - (\displaystyle\sum_{i=1}^{n} x_i)^2 } $$ 

and 

$$ b = { \displaystyle\sum_{i=1}^{n} y_i \displaystyle\sum_{i=1}^{n} x_i^2 - \displaystyle\sum_{i=1}^{n} x_i \displaystyle\sum_{i=1}^{n} x_iy_i  \over  n\displaystyle\sum_{i=1}^{n} x_i^2 - (\displaystyle\sum_{i=1}^{n} x_i)^2 } $$

*source*: https://zcht.home.amu.edu.pl/pliki/Regresja%20liniowa.pdf, https://en.wikibooks.org/wiki/LaTeX/Mathematics

They use 5 intermediate formulas:

$n$ = **period_count** - equivalent of SQL $count(period)$ assuming that all values are distinct for a sensor

$\displaystyle\sum_{i=1}^{n} x_iy_i$ = **sum_period_temp** - a sum of multiplies of period and temperature, i.e. $sum(period * temperature)$

$\displaystyle\sum_{i=1}^{n} x_i$ = **sum_period** - a sum of all period numbers, i.e. $sum(period)$

$\displaystyle\sum_{i=1}^{n} y_i$ = **sum_temp** - a sum of all temperatures, i.e. $sum(temp)$

$\displaystyle\sum_{i=1}^{n} x_i^2$ = **sum_period_sqr** - a sum of squares of period, i.e $sum(period^2)$

Note that all of them are additive, so we can calculate them incrementally. They could be reduced to 3 additive formulas (2 numerators and 1 common denominator), but I leave them as they refer directly to the source formula. Also I need some complexity to demonstrate different approaches to solving the problem.

Given all the above we get:

    lin_reg_a = (period_count * sum_period_temp - sum_period * sum_temp) / 
                (period_count * sum_period_sqr - sum_period * sum_period)

    lin_reg_b = (sum_temp * sum_period_sqr - sum_period * sum_period_temp) / 
                (period_count * sum_period_sqr - sum_period * sum_period)
        
    next_temp_extrapl = lin_reg_a * next_period + lin_reg_b


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

It's shorter, all we do here is calculation of one column at a time. This is much better to me. It is clearer and 
compile-time verifiable. Of course - the compiler will not correct typos in column names, but they can be replaced with constants.

One could argue that chaining *withColumn* functions degrades performance. This should be checked every time we perceive 
speed as a decisive factor. Quite often this will not be a case and if only the performance is satisfactory 
readability should be regarded as critical.

But this still isn't perfect (if anything is...)

Take a look at testability. In order to calculate anything through Spark you must instantiate a SparkSession object, 
run the process and eventually close the session. This all takes precious time. One spark-based test can take many seconds, 
sometimes even a few minutes.

This practically cancels fast TDD flow of Red/Green/Refactor, where we assume testing at milliseconds or seconds at most.

Is there something we can do about it?

## Spark take 3 - integration with Scala ##

How about this:

      def processSparkScala(baseFile:String, inputFile:String)(implicit spark:SparkSession):DataFrame= {
        val joined=getJoinedInputWithBase(baseFile,inputFile)
    
        import spark.implicits._
        val output=
          joined
            .as[BaseWithInput]
            .map(Base(_))
            .toDF()
    
        output.show()
        output
    }

Obviously this is not all, as the whole business logic is delegated out of spark-aware code. 

We use spark to load files, to do joins and necessary column operations (e.g. renaming).
but the difficult part is just pure Scala. The key here is mapping between two case classes: **BaseWithInput** and **Base**.
Spark is able to process records in a data frame in the following way:
1. For each "input" row (input to the "business logic" part of the application) spark creates an instance of Scala case class. 
The input case class should have a structure (i.e. list of fields) directly corresponding to spark data frame, 
including column names and data types. This will ensure smooth operation.
2. The developer should provide a function that accepts "input" case class as parameter and returns "output" case class.
This mapping functionality is completely separated from spark.
3. The "output" case class returned by function from pt.2. is read by spark to the data frame using field names and types as columns definition.

The code above shows no.1 (function: **as[T]**) and no.3. (function **map**).

So let's now look at pt.2.:

    object Base {
      def apply(joined:BaseWithInput):Base = {
        val nextPeriod=joined.input_period+1
        val periodCount=joined.period_count.getOrElse(0L) + 1
        val sumPeriodTemp=joined.sum_period_temp.getOrElse(0.0)+joined.input_period*joined.input_temp
        val sumPeriod=joined.sum_period.getOrElse(0L)+joined.input_period
        val sumTemp=joined.sum_temp.getOrElse(0.0)+joined.input_temp
        val sumPeriodSqr=joined.sum_period_sqr.getOrElse(0L) + joined.input_period * joined.input_period
    
        val denominator=periodCount * sumPeriodSqr - sumPeriod * sumPeriod
    
        val (linRegA,linRegB,nextTempExtrapl) = denominator match {
          case 0.0 => (None, None, None)
          case denominator =>
            val a=(periodCount * sumPeriodTemp - sumPeriod * sumTemp)/denominator
            val b=(sumTemp * sumPeriodSqr - sumPeriod * sumPeriodTemp)/denominator
            (Some(a),Some(b),Some(nextPeriod*a+b))
          }
    
        new Base(joined.sensor,joined.input_period,joined.input_temp,joined.next_temp_extrapl,
                 nextPeriod,nextTempExtrapl,linRegA,linRegB,
                 periodCount,sumPeriodTemp,sumPeriod,sumTemp,sumPeriodSqr
        )
      }
    }

You may argue (and you'll probably be right) that this is not the cleanest code ever. The point is you are not
bound by spark requirements, and you are free to write the code according to your style. 
You may even use other language. The only constraints are:
1. Input and output case classes must reflect spark data frames structures
2. Nullable fields should be optional (Option[T])
3. Data types should be either simple (numbers, text, date etc., but not other objects) or Arrays. 
Such complex fields work excellent with json files.

Apart from code itself the really important aspect of this approach is testability. You can write 
the TDD-style tests and execute them in milliseconds - they do not need costly spark session. 
The sample tests are in **ScalaTest** class:

    test("linear regression calculation test for a single period") {
        Given("base from previous period with input data")
        val joinedBaseWithInput=
        BaseWithInput(1,Some(3L),Some(17.81),Some(20.592),Some(4L),Some(19.1126666666667),Some(0.838999999999989),
        Some(15.7566666666667),Some(3L),Some(106.286),Some(6L),Some(52.304),Some(14L),4,20.787)
        
        When("current base is calculated")
        val newBase=Base(joinedBaseWithInput)
        
        Then("values are equal to reference data")
        val reference=new Base(1,4,20.787,Some(19.112667),5,Some(21.626),
        Some(1.3413),Some(14.9195),4,189.434,10,73.091,30)
        // compare two Base objects with aproppriate roundings
        assert(TestUtils.baseEquals(reference,newBase,6))
    }

This test is straightforward as the whole functionality is **Base.apply** function. You may, however, need
much more comprehensive tests and much broader code to work. Having the ability to follow TDD workflow and run tests
instantly every small change instead of waiting long seconds or even minutes for spark-generated results 
greatly improves developer's comfort and improves design.

## Run the application ##

The whole application can be executed in a very similar way to tests. The main difference is the location of base and input files
as well as saving new base. It is a good practice to keep separate files for each period as it is safer. If you really need one file, 
you should consider partitioning (in this case by period) and dynamic partition overwrite mode:

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

File locations can be passed via program arguments, environment variables, can be read from a database etc.
If you just want to play with sample data, the shortest loop calculating consecutive period can be as simple as:

    import FileLoader._
    for(period <- List.range(1,12)) {
      SparkModel
        .processSparkScala(f"data/base_${period-1}",f"data/input_$period.csv")
        .saveCSV(f"data/base_$period")
    }

Note that **saveCSV** function is not part of spark API, this is just a custom function wrapped in an *implicit class* 
(see FileLoader). This is a great scala feature that can be used to make the code more expressive.
