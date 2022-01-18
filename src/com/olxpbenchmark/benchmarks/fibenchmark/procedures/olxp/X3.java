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

public class X3 extends Procedure {

    public final SQLStmt GetAccount = new SQLStmt(
            "SELECT * FROM " + FIConstants.TABLENAME_ACCOUNTS +
                    " WHERE name = ?"
    );

    public final SQLStmt UpdateCheckingBalance = new SQLStmt(
            "UPDATE " + FIConstants.TABLENAME_CHECKING +
                    "   SET bal = bal + ? " +
                    " WHERE custid = ?"
    );

    public final SQLStmt query_stmt = new SQLStmt(
            "select MIN(bal) "
                    + " from "
                    +  FIConstants.TABLENAME_CHECKING
    );

    public void run(Connection conn, String custName, double amount) throws SQLException {
        // First convert the custName to the custId
        PreparedStatement stmt0 = this.getPreparedStatement(conn, GetAccount, custName);
        ResultSet r0 = stmt0.executeQuery();
        if (r0.next() == false) {
            String msg = "Invalid account '" + custName + "'";
            throw new UserAbortException(msg);
        }
        long custId = r0.getLong(1);
        r0.close();
        r0 = null;

        stmt0 = this.getPreparedStatement(conn, query_stmt);
        r0 = stmt0.executeQuery();
        assert(r0 != null);
        r0.close();
        r0=null;

        // Then update their checking balance
        PreparedStatement stmt1 = this.getPreparedStatement(conn, UpdateCheckingBalance, amount, custId);
        int status = stmt1.executeUpdate();
        assert(status == 1) :
                String.format("Failed to update %s for customer #%d [amount=%.2f]",
                        FIConstants.TABLENAME_CHECKING, custId, amount);

        return;
    }
}

