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

package com.ica.customer.implementation

import com.ica.customer.session.SessionInit
import com.ica.customer.info.{CustomerFlatten, CustomerOfferFlatten, CustomerTransactionFlatten}
import com.ica.customer.utilities.CustomerUtilities
import java.util.Properties

import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.io.Source
object CustomerMain extends SessionInit {

  val prop = new Properties()
  val propertiesPath = getClass.getResource("/input.properties")
  if(propertiesPath != null) {
    val input = Source.fromURL(propertiesPath)
    prop.load(input.bufferedReader())
  }
  val customerInfoPath = prop.getProperty("customer_info_data")
  val customerOfferInfo = prop.getProperty("customer_offer_data")
  val weekOfYears = prop.getProperty("week_number")
  val transactionData = prop.getProperty("customer_transaction_data")
  /** Schema of Customer Info */
  val customerInfoSchema = StructType(Array(StructField("cust_id", StringType, true),
    StructField("cust_name", StringType, true),
    StructField("cust_dob", StringType, true),
    StructField("gender",StringType,true)))
  /** Schema for Customer Offer data */
  val customerOfferSchema = StructType(Array(StructField("offer_id", StringType, true),
    StructField("offer_name", StringType, true),
    StructField("cust_id", StringType, true),
    StructField("begin_date", StringType, true),
    StructField("end_date",StringType,true)))
  /** Schema for Customer Transaction data */
  val customerTransactionSchema = StructType(Array(StructField("transaction_id", StringType, true),
    StructField("customer_id", StringType, true),
    StructField("product_id", StringType, true),
    StructField("store_id",StringType,true),
    StructField("offer_id",StringType,true),
    StructField("sales",StringType,true),
    StructField("date",StringType,true)))

  def main(args:Array[String]): Unit = {

    /** Flatten Customer Info */
    val getCustomerInfoDataFrame = CustomerFlatten.getCustomerInfoDataFrame(sprk,customerInfoPath,customerInfoSchema)
    getCustomerInfoDataFrame.show()
    val customerCall = CustomerFlatten.callCustomerInfo(getCustomerInfoDataFrame)
    /** Flatten Customer Offer Details */
    val getCustomerOfferInfo = CustomerOfferFlatten.getCustomerOfferDataFrame(customerOfferInfo,customerOfferSchema)
    val getWeeks = CustomerOfferFlatten.getWeeksOfYear(weekOfYears)
    val customerOffers = CustomerOfferFlatten.customerOfferCall(getCustomerOfferInfo,getWeeks)
    /** Flatten customer transaction details*/
    val getCustomerTransaction = CustomerTransactionFlatten.getTransactionDataFrame(transactionData,customerTransactionSchema)
    val transactionDetailsOfCustomer = CustomerTransactionFlatten.callCustomerTransaction(getCustomerTransaction)
    /** Fetching each customer's weekly status */
    val customersWeeklyStatus = CustomerUtilities.customerWiseweeklyStatus(customerCall,customerOffers,transactionDetailsOfCustomer)
    customersWeeklyStatus.show()
    /** Fetching overall weekly status */
    val overAllWeeklyStatus = CustomerUtilities.customerOverAllWeeklyStatus(customersWeeklyStatus)
    overAllWeeklyStatus.show()
    /** Fetching customer's age groupwise details */
    val agewiseTransaction = CustomerUtilities.customerTransactionResult(transactionDetailsOfCustomer,customerCall)
    val transactionOnAgeGroupWise = CustomerUtilities.transactionOnAgeGroupWise(agewiseTransaction)
    transactionOnAgeGroupWise.show()
  }
}
