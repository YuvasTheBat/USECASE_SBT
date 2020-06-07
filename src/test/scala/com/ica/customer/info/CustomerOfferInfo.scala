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
class CustomerOfferInfo extends FunSuite  with SessionInit {
   val customerOfferDataInvalidPath = "C:\\Users\\yuvas\\Azure_Study\\VISA\\EASy\\SOURCE\\input\\offerInvalid.csv"
   val customerOfferDataValidPath = "C:\\Users\\yuvas\\Azure_Study\\VISA\\EASy\\SOURCE\\input\\offer.csv"
   val  weeksOfTheYear = "C:\\Users\\yuvas\\Azure_Study\\VISA\\EASy\\SOURCE\\input\\offer.csv"

  val customerOfferSchema = StructType(Array(StructField("offer_id", StringType, true),
    StructField("offer_name", StringType, true),
    StructField("cust_id", StringType, true),
    StructField("begin_date", StringType, true),
    StructField("end_date",StringType,true)))

  val invalidSchema = StructType(Array(StructField("offer_id", StringType, true),
    StructField("offer_name", StringType, true),
    StructField("begin_date", StringType, true),
    StructField("end_date",StringType,true)))

  import sprk.implicits._
  val customer_offer = List(("10001","winter_sale","2","2020-02-02","2020-02-14")
    ,("10001","winter_sale","3","2020-02-02","2020-02-14")).toDF("offer_id","offer_name","cust_id","begin_date","end_date")
  val weekly = List(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,
    33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53)
    .toDF("week_number")

  val expected = List((6,"10001","winter_sale","2","2020-02-02","2020-02-14",6,7),
    (7,"10001","winter_sale","2","2020-02-02","2020-02-14",6,7),
    (6,"10001","winter_sale","3","2020-02-02","2020-02-14",6,7),
    (7,"10001","winter_sale","3","2020-02-02","2020-02-14",6,7))
    .toDF("week_number","offer_id","offer_name","cust_id","begin_date","end_date","begin_week_number","end_week_number")

  val expectedFail = List((6,"10001","winter_sale","2","2020-02-02","2020-02-14",6,7),
    (7,"10001","winter_sale","2","2020-02-02","2020-02-14",6,7),
    (6,"10001","winter_sale","2","2020-02-02","2020-02-14",6,7),
    (7,"10001","winter_sale","3","2020-02-02","2020-02-14",6,7),
    (8,"10001","winter_sale","3","2020-02-02","2020-02-14",6,7))
    .toDF("week_number","offer_id","offer_name","cust_id","begin_date","end_date","begin_week_number","end_week_number")

  test("Testing for FileNotFound exception") {
    val thrown = intercept[FileNotFoundException] {
      CustomerOfferFlatten.getCustomerOfferDataFrame(customerOfferDataInvalidPath,customerOfferSchema)
    }
    assert(thrown.getMessage.contains("FileNotFound"))
  }

  test("Testing for FileNotFound exception of number table") {
    val thrown = intercept[FileNotFoundException] {
      CustomerOfferFlatten.getWeeksOfYear(weeksOfTheYear)
    }
    assert(thrown.getMessage.contains("FileNotFound For Numbers Table"))
  }

  test("Testing for valid customer offer path") {
    val customerOfferData = CustomerOfferFlatten.getCustomerOfferDataFrame(customerOfferDataValidPath,customerOfferSchema)
    assert(customerOfferData.count() > 0)
  }
  test("Unit test for invalid customer offer data") {
    val invalidSchemaForCustomerOffer = CustomerOfferFlatten.getCustomerOfferDataFrame(customerOfferDataValidPath,invalidSchema)
    assert(invalidSchemaForCustomerOffer.rdd.isEmpty())
  }
  test("Test case for check customer offer dataframe contains new columns after computation") {
    val actual = CustomerOfferFlatten.customerOfferCall(customer_offer, weekly)
    assert(actual.columns.contains("begin_week_number") && actual.columns.contains("end_week_number"))
  }
  test("Unit test case for compare actual and extected data of customer offer") {
    val actual = CustomerOfferFlatten.customerOfferCall(customer_offer, weekly)
    val newDF = actual.except(expected)
    assert(newDF.rdd.isEmpty())
  }
  test("Fail_Scenrio : Unit test case for compare actual and extected data of customer offer") {
    val actual = CustomerOfferFlatten.customerOfferCall(customer_offer, weekly)
    val newDF = expectedFail.except(actual)
    assert(!newDF.rdd.isEmpty())
  }
}
