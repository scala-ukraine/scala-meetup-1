package com.github.scalaukraine

import org.apache.spark.sql.{Encoder, SparkSession}
import io.delta.tables.DeltaTable

object Main {
  case class Person(name: String, age: Int)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("deltalake")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    sys.addShutdownHook {
      spark.close()
    }

    import spark.implicits._

    val people = List.tabulate(100)(n => Person(s"Name #$n", n)).toDF()

    val peopleSchema = implicitly[Encoder[Person]].schema

    val exampleTable = DeltaTable
      .createIfNotExists(spark)
      .tableName("people")
      .location("example-lake")
      .addColumns(peopleSchema)
      .execute()

    exampleTable
      .as("existing")
      .merge(people.as("new"), $"existing.name" === $"new.name")
      .whenNotMatched()
      .insertAll()
      .execute()
  }
}
