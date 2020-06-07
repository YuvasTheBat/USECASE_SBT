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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

import scala.util.{Failure, Success, Try}
import java.io.FileNotFoundException

import com.ica.customer.session.SessionInit
import org.apache.spark.sql.functions._
import java.sql.Date
import java.text.SimpleDateFormat
import java.text.ParseException

import org.apache.spark.sql.types.{StringType, StructField, StructType}
//import java.io.IOException

object CustomerFlatten extends SessionInit {

  private val MIN_20_AGE_GROUP = 15
  private val MAX_20_AGE_GROUP = 20
  private val MIN_25_AGE_GROUP = 21
  private val MAX_25_AGE_GROUP = 25
  private val MAX_30_AGE_GROUP = 31
  private val HEADER_FOR_CUSTOMER_DATA = true

  /**
   * Method to convert customer raw data into dataframe
   * @param sprk
   * @param pathC
   * @return customerInfo
   */
  def getCustomerInfoDataFrame(sprk:SparkSession,pathC:String,schema:StructType) : DataFrame = {
    //cust_id,cust_name,cust_dob,gender
    val customerInfo = Try({
      val customerFlat = sprk.read.format("CSV").schema(schema).option("mode", "DROPMALFORMED")
        .csv(pathC)
      customerFlat
    })
    customerInfo match {
      case Success(v) => v
      case Failure(issue) =>
        throw new FileNotFoundException("FileNotFound for customer's info")
    }
  }

  private val timesec = System.currentTimeMillis()
  val currentDate = new Date(timesec)
  val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
  dateFormat.setLenient(false)
    val getAge = udf((dob:String) => {
    Try {
      val dateUtil = dateFormat.parse(dob)
      val dobDate = new Date(dateUtil.getTime)
      currentDate.getYear - dobDate.getYear
    } match {
      case Success(v) => v.toInt
      case Failure(issue) =>
        throw new ParseException(dob, currentDate.getYear)
        0
    }
  })

  /**
   * Method to flatten customer info data and add AGE & AGE_GROUP columns.
   * @param customerFlat
   * @return resultCustomer
   */
  def callCustomerInfo(customerFlat:DataFrame) : DataFrame = {
    val custInfo = customerFlat.withColumn("age", getAge(col("cust_dob")))
    val resultCustomer = custInfo.withColumn("age_group", when(col("age").>=(MIN_20_AGE_GROUP) && col("age").<=(MAX_20_AGE_GROUP), "20_age_group")
      .when(col("age").>(MIN_25_AGE_GROUP) && col("age").<=(MAX_25_AGE_GROUP), "25_age_group")
      .when(col("age").>(MAX_25_AGE_GROUP) && col("age").<=(MAX_30_AGE_GROUP), "30_age_group")
      .otherwise("unknown"))
    resultCustomer
  }
}
