/*
 * Copyright 2021 OLxPBench

 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

 *  http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.

 */

package com.olxpbenchmark.benchmarks.fibenchmark.procedures.olxp;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.olxpbenchmark.api.Procedure;
import com.olxpbenchmark.api.SQLStmt;
import com.olxpbenchmark.benchmarks.fibenchmark.FIConstants;

public class X5 extends Procedure {

    public final SQLStmt GetAccount = new SQLStmt(
            "SELECT * FROM " + FIConstants.TABLENAME_ACCOUNTS +
                    " WHERE name = ?"
    );

    public final SQLStmt GetSavingsBalance = new SQLStmt(
            "SELECT bal FROM " + FIConstants.TABLENAME_SAVINGS +
                    " WHERE custid = ?"
    );

    public final SQLStmt UpdateSavingsBalance = new SQLStmt(
            "UPDATE " + FIConstants.TABLENAME_SAVINGS +
                    "   SET bal = bal - ? " +
                    " WHERE custid = ?"
    );

    public final SQLStmt query_stmt = new SQLStmt(
            "select MAX(bal) "
                    + " from "
                    +  FIConstants.TABLENAME_SAVINGS
    );

    public void run(Connection conn, String custName, double amount) throws SQLException {
        // First convert the custName to the acctId
        PreparedStatement stmt = this.getPreparedStatement(conn, GetAccount, custName);
        ResultSet result = stmt.executeQuery();
        if (result.next() == false) {
            String msg = "Invalid account '" + custName + "'";
            throw new UserAbortException(msg);
        }
        long custId = result.getLong(1);
        result.close();


        stmt = this.getPreparedStatement(conn, query_stmt);
        result = stmt.executeQuery();
        assert(result != null);
        result.close();


        // Get Balance Information
        stmt = this.getPreparedStatement(conn, GetSavingsBalance, custId);
        result = stmt.executeQuery();
        if (result.next() == false) {
            String msg = String.format("No %s for customer #%d",
                    FIConstants.TABLENAME_SAVINGS,
                    custId);
            throw new UserAbortException(msg);
        }
        double balance = result.getDouble(1) - amount;
        result.close();

        // Make sure that they have enough
        if (balance < 0) {
            String msg = String.format("Negative %s balance for customer #%d",
                    FIConstants.TABLENAME_SAVINGS,
                    custId);
            throw new UserAbortException(msg);
        }

        // Then update their savings balance
        stmt = this.getPreparedStatement(conn, UpdateSavingsBalance, amount, custId);
        int status = stmt.executeUpdate();
        assert(status == 1) :
                String.format("Failed to update %s for customer #%d [balance=%.2f / amount=%.2f]",
                        FIConstants.TABLENAME_CHECKING, custId, balance, amount);

        return;
    }
}
