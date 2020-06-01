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

package com.ica.customer.utilities

import scala.util.{Failure, Success, Try}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType,DoubleType}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import com.ica.customer.session.SessionInit

object CustomerUtilities extends SessionInit {

  /**
   * Method to find weekwise transaction of each customer's.
   * @param customerCall
   * @param customerOfferCall
   * @param transactionRes
   * @return resultOfWeeklyTransaction
   */
  def customerWiseweeklyStatus(customerCall: DataFrame, customerOfferCall: DataFrame, transactionRes: DataFrame): DataFrame = {

    val transactionFlatten =
      transactionRes.select(col("sales_week_number"), col("sales").cast(DoubleType), col("customer_id"), col("offer_id").alias("offer_redeemed"))
      .join(
        customerOfferCall.select(col("week_number"), col("offer_id").alias("offer_received"), col("cust_id")),
        transactionRes("sales_week_number") === customerOfferCall("week_number") && transactionRes("customer_id") === customerOfferCall("cust_id"), "left")
      .select("sales_week_number", "sales", "customer_id", "offer_received", "offer_redeemed")

    val transactionFlattenForWeeklyStatus =
      transactionFlatten.join(customerCall.select(col("age"), col("age_group"),
        col("cust_id")), transactionFlatten("customer_id") === customerCall("cust_id"), "left")
      .select("sales_week_number", "sales", "customer_id", "offer_received", "offer_redeemed", "age", "age_group")

    val resultOfWeeklyTransaction =
      transactionFlattenForWeeklyStatus.groupBy("sales_week_number", "customer_id", "age", "age_group")
      .agg(sum("sales").as("sales_sum"), count("offer_received").as("offer_received_count"),
        count("offer_redeemed").as("offer_redeemed_count"), count("sales_week_number").as("no_of_visit"))
      .orderBy(col("sales_week_number").asc, col("customer_id").cast(IntegerType).asc)
    resultOfWeeklyTransaction
  }

  /**
   * Method for derive over all transaction status of every week.
   * @param customerWiseweeklyStatus
   * @return customerOverAllWeeklyStatus
   */
  def customerOverAllWeeklyStatus(customerWiseweeklyStatus: DataFrame): DataFrame = {
    Try {
      val customerOverAllWeeklyStatus = customerWiseweeklyStatus.groupBy("sales_week_number").agg(
        count("customer_id").cast(IntegerType).as("customer_count"),
        sum("offer_received_count").as("total_offer_received"), sum("offer_redeemed_count")
          .as("total_offer_redeemed"), sum("sales_sum")
          .as("total_amount"), sum("no_of_visit")
          .as("no_of_visit"))
        .orderBy(col("sales_week_number").cast(IntegerType).asc) //.show()

      customerOverAllWeeklyStatus
    }
    match {
      case Success(v)=>v
      case Failure(issue) =>
        throw new NullPointerException("customerWiseweeklyStatus is null")
    }

  }

  def customerTransactionResult(transaction: DataFrame, customerCall: DataFrame): DataFrame = {

    val transactionResult =
      transaction.join(customerCall, transaction("customer_id") === customerCall("cust_id"), "inner")
        .select("transaction_id", "customer_id", "product_id", "store_id", "offer_id",
          "sales", "date", "sales_week_number", "day_of_sales_week", "age_group")
    transactionResult
  }

  /**
   * Method to bring agewise transaction of the customer's
   * @param transactionAgeGroup
   * @return resultAgeGroup
   */
  def transactionOnAgeGroupWise(transactionAgeGroup: DataFrame): DataFrame = {
    Try {
      val resultOfAgeGroup = transactionAgeGroup.groupBy("day_of_sales_week", "age_group")
          .agg(count("day_of_sales_week").as("days_count"))
      val resultOfAgeGroupFetch = resultOfAgeGroup.groupBy("age_group").agg(max("days_count").as("total"))
      val resultAgeGroup =
        resultOfAgeGroup.join(resultOfAgeGroupFetch, resultOfAgeGroup("age_group") === resultOfAgeGroupFetch("age_group")
          && resultOfAgeGroup("days_count") === resultOfAgeGroupFetch("total"))
        .select(resultOfAgeGroup("day_of_sales_week"), resultOfAgeGroup("age_group"), resultOfAgeGroupFetch("total"))
      resultAgeGroup
    }
    match {
      case Success(v)=>v
      case Failure(issue) =>
        throw new NullPointerException("transactionAgeGroup is null")
    }
  }

}
