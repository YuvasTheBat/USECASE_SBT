// Copyright (C) 2011-2012 the original author or authors.
// See the LICENCE.txt file distributed with this work for additional
// information regarding copyright ownership.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.ica.customer.info

import java.io.FileNotFoundException

import com.ica.customer.session.SessionInit
import org.apache.spark.SparkException
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import com.ica.customer.implementation.CustomerMain

@RunWith(classOf[JUnitRunner])
class CustomerInfo extends FunSuite with SessionInit {

  val schema = StructType(Array(StructField("cust_id", StringType, true), StructField("cust_name", StringType, true)
    , StructField("cust_dob", StringType, true),StructField("gender",StringType,true)))
  val invalidSchema = StructType(Array(StructField("cust_id", StringType, true), StructField("cust_name", StringType, true)
    ,StructField("gender",StringType,true)))
  val pathCustomerData = "C:\\Users\\yuvas\\Azure_Study\\VISA\\EASy\\SOURCE\\input\\dobcustomerfortest.csv"
  val pathCustomerDataValid = "C:\\Users\\yuvas\\Azure_Study\\VISA\\EASy\\SOURCE\\input\\dob.csv"
  val age_check = 31
  test("Unit test case on FileNotFoundException for customers data") {
    val thrown = intercept[FileNotFoundException] {
      CustomerMain.main(null)
      CustomerFlatten.getCustomerInfoDataFrame(sprk,pathCustomerData,schema)
    }
    assert(thrown.getMessage.contains("FileNotFound"))
  }

  test("Unit test case on valid path for customers data") {
      val customerData = CustomerFlatten.getCustomerInfoDataFrame(sprk,pathCustomerDataValid,schema)
      assert(customerData.count() > 0)
    }

  test("Unit test for check invalid schema") {
    val customerDataForInvalidSchema = CustomerFlatten.getCustomerInfoDataFrame(sprk,pathCustomerDataValid,invalidSchema)
    customerDataForInvalidSchema.show()
    assert(customerDataForInvalidSchema.rdd.isEmpty())
  }
  test("Unit testing for check new columns are in actual dataframe or not") {
    import sprk.implicits._
    val list_input = List(("1", "yuvas", "1989-06-22", "M"), ("2", "murugan", "1989-09-23", "M"), ("3", "kamali", "1992-06-24", "F"))
    val input = sprk.sparkContext.parallelize(list_input).toDF("cust_id", "cust_name", "cust_dob", "gender")
    val actual = CustomerFlatten.callCustomerInfo(input)
    assert(actual.columns.contains("age") && actual.columns.contains("age_group"))
  }

  test ("Unit test case for test ParseException while passing wrong date format") {
    import sprk.implicits._
    val list_input = List(("1", "yuvas", "24-06-1992", "M"))
    val input = sprk.sparkContext.parallelize(list_input).toDF("cust_id", "cust_name", "cust_dob", "gender")
    val thrown = intercept[SparkException] {
      val ageFind = input.withColumn("AGE", CustomerFlatten.getAge(input.col("cust_dob")))
      ageFind.show()
    }
    assert(thrown.getMessage.contains("ParseException"))
  }
  test("Unit test case for correct date format") {
    import sprk.implicits._
    val list_input = List(("1", "viki", "1989-04-26", "M"))
    val input = sprk.sparkContext.parallelize(list_input).toDF("cust_id", "cust_name", "cust_dob", "gender")
    val ageFind = input.withColumn("AGE", CustomerFlatten.getAge(input.col("cust_dob")))
    assert(ageFind.select(col("AGE")).first().getInt(0).equals(age_check))
  }
}
