### Tips
1. Separate Data reading and Data processing, so they can be tested individually without passing the spark session.
1. Split processing into smaller transform steps which can be individually tested.
1. Prefer column functions to user defined functions (udf) as the former can be optimised by query planner.
1. Easier to test a native scala function than an udf.
 
### Running Tests
`sbt test`

### Test Performance
1. A single spark session is reused to avoid starting time, but might cause issues if sql tables are being registered in the tests. (See `SparkSessionTestWrapper`)
1. The parameters for jvm and test runner need to be tweaked. (See `build.sbt`)

### Per Test Spark
We can use the loan pattern to initiate and teardown a spark instance per test.
This might be needed if tests are registering same views.
(See `LoanedSparkSession`)
