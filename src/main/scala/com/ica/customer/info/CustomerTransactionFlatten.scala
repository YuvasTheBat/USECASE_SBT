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

import scala.util.{Failure, Success, Try}
import org.apache.spark.sql.functions.{col, date_format}
import org.apache.spark.sql.types.{IntegerType, StructType}
import com.ica.customer.session.SessionInit
import org.apache.spark.sql.{DataFrame, types}
object CustomerTransactionFlatten extends SessionInit {

  private val HEADER_FOR_TRANSACTION_DATA = true

  /**
   * Method to convert raw data of customer's transactions into datframe
   * @param pathC
   * @return transactionFlat
   */
  def getTransactionDataFrame(pathC:String,schema:StructType) : DataFrame = {
    val transaction = Try ({
      val  transactionFlat = sprk.read.format("CSV").schema(schema).option("mode", "DROPMALFORMED")
        .csv(pathC)
      transactionFlat
    })
    transaction match {
      case Success(v) => v
      case Failure(issue) =>
        throw new FileNotFoundException("FileNotFound for Customer Transaction")

    }
  }

  /**
   * Methos to include new columns like sales_week_number and day_of_sales_week.
   * @param transactionFlat
   * @return transaction
   */
  def callCustomerTransaction(transactionFlat:DataFrame) : DataFrame = {
    val transaction = transactionFlat.withColumn("sales_week_number", date_format(col("date"), "w").cast(IntegerType))
      .withColumn("day_of_sales_week", date_format(col("date"), "E"))
    transaction
  }

}
