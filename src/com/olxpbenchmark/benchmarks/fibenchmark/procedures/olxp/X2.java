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

public class X2 extends Procedure {

    public final SQLStmt GetAccount = new SQLStmt(
            "SELECT * FROM " + FIConstants.TABLENAME_ACCOUNTS +
                    " WHERE name = ?"
    );

    public final SQLStmt query_stmt = new SQLStmt(
            "select COUNT(name) "
                    + " from "
                    +  FIConstants.TABLENAME_ACCOUNTS
    );

    public final SQLStmt GetSavingsBalance = new SQLStmt(
            "SELECT bal FROM " + FIConstants.TABLENAME_SAVINGS +
                    " WHERE custid = ?"
    );

    public final SQLStmt GetCheckingBalance = new SQLStmt(
            "SELECT bal FROM " + FIConstants.TABLENAME_CHECKING +
                    " WHERE custid = ?"
    );

    public double run(Connection conn, String custName) throws SQLException {
        // First convert the acctName to the acctId
        PreparedStatement stmt0 = this.getPreparedStatement(conn, GetAccount, custName);
        ResultSet r0 = stmt0.executeQuery();
        if (r0.next() == false) {
            String msg = "Invalid account '" + custName + "'";
            throw new UserAbortException(msg);
        }
        long custId = r0.getLong(1);

        PreparedStatement stmtq = this.getPreparedStatement(conn, query_stmt);
        ResultSet q0 = stmtq.executeQuery();
        assert(q0 != null);
        q0.close();

        // Then get their account balances
        PreparedStatement balStmt0 = this.getPreparedStatement(conn, GetSavingsBalance, custId);
        ResultSet balRes0 = balStmt0.executeQuery();
        if (balRes0.next() == false) {
            String msg = String.format("No %s for customer #%d",
                    FIConstants.TABLENAME_SAVINGS,
                    custId);
            throw new UserAbortException(msg);
        }

        PreparedStatement balStmt1 = this.getPreparedStatement(conn, GetCheckingBalance, custId);
        ResultSet balRes1 = balStmt1.executeQuery();
        if (balRes1.next() == false) {
            String msg = String.format("No %s for customer #%d",
                    FIConstants.TABLENAME_CHECKING,
                    custId);
            throw new UserAbortException(msg);
        }

        return (balRes0.getDouble(1) + balRes1.getDouble(1));
    }
}

