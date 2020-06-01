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
import com.ica.customer.info.{CustomerFlatten,CustomerOfferFlatten,CustomerTransactionFlatten}
import com.ica.customer.utilities.CustomerUtilities
object CustomerMain extends SessionInit {

  def main(args:Array[String]): Unit = {
    val customerInfoPath = "C:\\Users\\yuvas\\Azure_Study\\VISA\\EASy\\SOURCE\\input\\dob.csv"
    val customerOfferInfo = "C:\\Users\\yuvas\\Azure_Study\\VISA\\EASy\\SOURCE\\input\\offer.csv"
    val weekOfYears = "C:\\Users\\yuvas\\Azure_Study\\VISA\\EASy\\SOURCE\\input\\number.csv"
    val transactionData = "C:\\Users\\yuvas\\Azure_Study\\VISA\\EASy\\SOURCE\\input\\transaction.csv"
    /** Flatten Customer Info */
    val getCustomerInfoDataFrame = CustomerFlatten.getCustomerInfoDataFrame(sprk,customerInfoPath)
    getCustomerInfoDataFrame.show()
    val customerCall = CustomerFlatten.callCustomerInfo(getCustomerInfoDataFrame)
    customerCall.show()
    /** Flatten Customer Offer Details */
    val getCustomerOfferInfo = CustomerOfferFlatten.getCustomerOfferDataFrame(customerOfferInfo)
    val getWeeks = CustomerOfferFlatten.getWeeksOfYear(weekOfYears)
    val customerOffers = CustomerOfferFlatten.customerOfferCall(getCustomerOfferInfo,getWeeks)
    customerOffers.show()
    /** Flatten customer transaction details*/
    val getCustomerTransaction = CustomerTransactionFlatten.getTransactionDataFrame(transactionData)
    val transactionDetailsOfCustomer = CustomerTransactionFlatten.callCustomerTransaction(getCustomerTransaction)
    transactionDetailsOfCustomer.show()

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
