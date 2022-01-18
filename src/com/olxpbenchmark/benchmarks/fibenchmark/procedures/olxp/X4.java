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

public class X4 extends Procedure {

    public final SQLStmt query_stmt = new SQLStmt(
            "select MAX(bal) "
                    + " from "
                    +  FIConstants.TABLENAME_CHECKING
    );

    public final SQLStmt GetAccount = new SQLStmt(
            "SELECT * FROM " + FIConstants.TABLENAME_ACCOUNTS +
                    " WHERE custid = ?"
    );

    public final SQLStmt GetCheckingBalance = new SQLStmt(
            "SELECT bal FROM " + FIConstants.TABLENAME_CHECKING +
                    " WHERE custid = ?"
    );

    public final SQLStmt UpdateCheckingBalance = new SQLStmt(
            "UPDATE " + FIConstants.TABLENAME_CHECKING +
                    "   SET bal = bal + ? " +
                    " WHERE custid = ?"
    );

    public void run(Connection conn, long sendAcct, long destAcct, double amount) throws SQLException {
        // Get Account Information
        PreparedStatement stmt0 = this.getPreparedStatement(conn, GetAccount, sendAcct);
        ResultSet r0 = stmt0.executeQuery();
        if (r0.next() == false) {
            String msg = "Invalid account '" + sendAcct + "'";
            throw new UserAbortException(msg);
        }

        PreparedStatement stmt1 = this.getPreparedStatement(conn, GetAccount, destAcct);
        ResultSet r1 = stmt1.executeQuery();
        if (r1.next() == false) {
            String msg = "Invalid account '" + destAcct + "'";
            throw new UserAbortException(msg);
        }

        PreparedStatement stmtq = this.getPreparedStatement(conn, query_stmt);
        ResultSet q0 = stmtq.executeQuery();
        assert(q0 != null);

        // Get the sender's account balance
        PreparedStatement balStmt0 = this.getPreparedStatement(conn, GetCheckingBalance, sendAcct);
        ResultSet balRes0 = balStmt0.executeQuery();
        if (balRes0.next() == false) {
            String msg = String.format("No %s for customer #%d",
                    FIConstants.TABLENAME_CHECKING,
                    sendAcct);
            throw new UserAbortException(msg);
        }
        double balance = balRes0.getDouble(1);

        // Make sure that they have enough money
        if (balance < amount) {
            String msg = String.format("Insufficient %s funds for customer #%d",
                    FIConstants.TABLENAME_CHECKING, sendAcct);
            throw new UserAbortException(msg);
        }

        // Debt
        PreparedStatement updateStmt = this.getPreparedStatement(conn, UpdateCheckingBalance, amount*-1d, sendAcct);
        int status = updateStmt.executeUpdate();
        assert(status == 1) :
                String.format("Failed to update %s for customer #%d [amount=%.2f]",
                        FIConstants.TABLENAME_CHECKING, sendAcct, amount);

        // Credit
        updateStmt = this.getPreparedStatement(conn, UpdateCheckingBalance, amount, destAcct);
        status = updateStmt.executeUpdate();
        assert(status == 1) :
                String.format("Failed to update %s for customer #%d [amount=%.2f]",
                        FIConstants.TABLENAME_CHECKING, destAcct, amount);

        return;
    }
}
