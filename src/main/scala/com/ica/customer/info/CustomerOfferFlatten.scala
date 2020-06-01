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

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.{Failure, Success, Try}
import java.io.FileNotFoundException
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.date_format
import com.ica.customer.session.SessionInit
import org.apache.spark.sql.types.IntegerType


object CustomerOfferFlatten extends SessionInit {

  private val HEADER_FOR_CUSTOMER_OFFER_DATA = true
  private val HEADER_FOR_NUMBER_TABLE = true

  /**
   * Method to convert raw data of customer's offer into dataframe and flatten with more ease
   * @param pathC
   * @return customerOfferFlat
   */
  def getCustomerOfferDataFrame(pathC:String) : DataFrame = {
    val customerOffer =  Try ({
      val customerOfferFlat = sprk.read.format("CSV")
        .option("header", HEADER_FOR_CUSTOMER_OFFER_DATA).option("mode", "DROPMALFORMED")
        .csv(pathC)
      customerOfferFlat
    })
    customerOffer match {
      case Success(v) => v
      case Failure(issue) =>
        issue.getMessage
        throw new FileNotFoundException("FileNotFound For Customer Offers")
    }
  }

  /**
   * Method to bring numbmer of weeks from years to calculate weekly transactions.
   * Adding new column called week_number.
   * @param pathC
   * @return numberTab
   */
  def getWeeksOfYear(pathC:String) : DataFrame = {
    val numberTab = Try ({
      val numberTabForYear = sprk.read.format("CSV")
        .option("header", HEADER_FOR_NUMBER_TABLE)
        .csv(pathC).select(col("AGE").alias("week_number").cast(IntegerType))
      numberTabForYear
    })
    numberTab match {
      case Success(v) => v
      case Failure(issue) =>
        issue.printStackTrace()
        throw new FileNotFoundException("FileNotFound For Numbers Table")
    }
  }

  /**
   * Method to flatten customer's offer with weekly informations.
   * Added new columns begin_week_number.begin_date,end_week_number,end_date
   * @param offerFlat
   * @param numberTab
   * @return customerOfferCall
   */
  def customerOfferCall(offerFlat:DataFrame,numberTab:DataFrame) : DataFrame = {
    val offer = offerFlat.withColumn("begin_week_number", date_format(col("begin_date"), "w").cast(IntegerType))
      .withColumn("end_week_number", date_format(col("end_date"), "w").cast(IntegerType))

    val customerOfferCall = offer.join(numberTab, numberTab("week_number")
      .between(offer("begin_week_number"), offer("end_week_number")))
      .select("week_number", "offer_id", "offer_name", "cust_id", "begin_date", "end_date", "begin_week_number", "end_week_number")
    customerOfferCall
  }

}
