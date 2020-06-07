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
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CustomerTransactionInfo extends  FunSuite with SessionInit {

  val pathCustomerTransactionData = "C:\\Users\\yuvas\\Azure_Study\\VISA\\EASy\\SOURCE\\input\\transaction.csv"
  val invalidPathOfTransactionData = "C:\\Users\\yuvas\\Azure_Study\\VISA\\EASy\\SOURCE\\input\\transactionInvalid.csv"

  val schema = StructType(Array(StructField("transaction_id", StringType, true),
    StructField("customer_id", StringType, true),
    StructField("product_id", StringType, true),
    StructField("store_id",StringType,true),
    StructField("offer_id",StringType,true),
    StructField("sales",StringType,true),
    StructField("date",StringType,true)))

  import sprk.implicits._
  private val list_input = List(("5001","2","2001","store_01","10001","2500.0","2020-02-15")
    ,("5003","3","3001","store_02","10002","5000.00","2020-02-24")
    ,("5004","2","4001","store_01","10003","3000.00","2020-03-16")
    ,("5009","3","3001","store_02","10002","5000.00","2020-02-20")).toDF("transaction_id","customer_id","product_id","store_id","offer_id","sales","date")

  test("Test for valid transaction details path") {
    val thrown = intercept[FileNotFoundException] {
      CustomerTransactionFlatten.getTransactionDataFrame(invalidPathOfTransactionData,schema)
    }
    assert(thrown.getMessage.contains("FileNotFound for Customer Transaction"))
  }

  test("Test for valid path of transaction data") {
    val getTransactionDataFrame = CustomerTransactionFlatten.getTransactionDataFrame(pathCustomerTransactionData,schema)
    assert(getTransactionDataFrame.count() > 0)
  }

  test("Test for find new columns are there in transaction's flattened dataframe") {
    val actual = CustomerTransactionFlatten.callCustomerTransaction(list_input)
    assert(actual.columns.contains("sales_week_number") && actual.columns.contains("day_of_sales_week"))
  }
}
