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
import com.olxpbenchmark.benchmarks.tabenchmark.TAConstants;

public class X1 extends Procedure {

    public final SQLStmt GetAccount = new SQLStmt(
            "SELECT * FROM " + FIConstants.TABLENAME_ACCOUNTS +
                    " WHERE custid = ?"
    );

    public final SQLStmt query_stmt = new SQLStmt(
            "select AVG(name) "
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

    public final SQLStmt UpdateSavingsBalance = new SQLStmt(
            "UPDATE " + FIConstants.TABLENAME_SAVINGS +
                    "   SET bal = bal - ? " +
                    " WHERE custid = ?"
    );

    public final SQLStmt UpdateCheckingBalance = new SQLStmt(
            "UPDATE " + FIConstants.TABLENAME_CHECKING +
                    "   SET bal = bal + ? " +
                    " WHERE custid = ?"
    );

    public final SQLStmt ZeroCheckingBalance = new SQLStmt(
            "UPDATE " + FIConstants.TABLENAME_CHECKING +
                    "   SET bal = 0.0 " +
                    " WHERE custid = ?"
    );

    public void run(Connection conn, long custId0, long custId1) throws SQLException {
        // Get Account Information
        PreparedStatement stmt0 = this.getPreparedStatement(conn, GetAccount, custId0);
        ResultSet r0 = stmt0.executeQuery();
        if (r0.next() == false) {
            String msg = "Invalid account '" + custId0 + "'";
            throw new UserAbortException(msg);
        }

        PreparedStatement stmtq = this.getPreparedStatement(conn, query_stmt);
        ResultSet q0 = stmtq.executeQuery();
        assert(q0 != null);
        q0.close();

        PreparedStatement stmt1 = this.getPreparedStatement(conn, GetAccount, custId1);
        ResultSet r1 = stmt1.executeQuery();
        if (r1.next() == false) {
            String msg = "Invalid account '" + custId1 + "'";
            throw new UserAbortException(msg);
        }

        // Get Balance Information
        PreparedStatement balStmt0 = this.getPreparedStatement(conn, GetSavingsBalance, custId0);
        ResultSet balRes0 = balStmt0.executeQuery();
        if (balRes0.next() == false) {
            String msg = String.format("No %s for customer #%d",
                    FIConstants.TABLENAME_SAVINGS,
                    custId0);
            throw new UserAbortException(msg);
        }

        PreparedStatement balStmt1 = this.getPreparedStatement(conn, GetCheckingBalance, custId1);
        ResultSet balRes1 = balStmt1.executeQuery();
        if (balRes1.next() == false) {
            String msg = String.format("No %s for customer #%d",
                    FIConstants.TABLENAME_CHECKING,
                    custId1);
            throw new UserAbortException(msg);
        }

        double total = balRes0.getDouble(1) + balRes1.getDouble(1);
        // assert(total >= 0);

        // Update Balance Information
        PreparedStatement updateStmt0 = this.getPreparedStatement(conn, ZeroCheckingBalance, custId0);
        int status = updateStmt0.executeUpdate();
        assert(status == 1);

        PreparedStatement updateStmt1 = this.getPreparedStatement(conn, UpdateSavingsBalance, total, custId1);
        status = updateStmt1.executeUpdate();
        assert(status == 1);

    }
}

