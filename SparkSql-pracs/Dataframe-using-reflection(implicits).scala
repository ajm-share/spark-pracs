val sqlContext = new org.apache.spark.sql.SQLContext(sc)

import sqlContext.implicits._


// Case Classes can support only 22 fields Its a Scala Limitation (Supposed to be fixed in Scala 2.11) 
case class Employee(id: Int, name: String, age: Int)
case class Person(name: String, age: Int)

val empl_iterim = sc.textFile("/home/aijaz/spark_pracs/datasets/employee.txt").map(_.split(","))

val people_iterim = sc.textFile("/home/aijaz/spark/spark-1.5.2-hadoop2.6/examples/src/main/resources/people.txt").map(_.split(","))

val	people = people_iterim.map(p => Person(p(0), p(1).trim.toInt)).toDF()

people.registerTempTable("people")




val empl = empl_iterim.map(e => Employee(e(0).toInt, e(1), e(2).toInt)).toDf() // This will return a DataFrame

empl.registerTempTable("employee") // This will register 'empl' as Table


// Two ways of querying 1) Using SqlContext.sql("Sql Query") ..2) Using Dataframes
// Both ways are shown for all operations

/* To Select All Employees */
sqlContext.sql("Select * from employee").show() 
empl.show()

/* Using Filter & where clause */
sqlContext.sql("Select * from employee where age > 25").show()
empl.filter(empl("age") > 25).show


/* Selecting a Particular column */
 sqlContext.sql("Select id from employee").show()
 empl.select("name").show

/* Taking Count by Grouping by Age */
 sqlContext.sql("Select age, count(*) from employee group by age").show()
empl.groupBy("age").count



/**/
val teenagers = sqlContext.sql("SELECT name, age FROM people WHERE age >= 13 AND age <= 19")
people.filter(people("age") >= 13 && people("age") <= 19).show