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

package com.olxpbenchmark.benchmarks.tabenchmark.procedures.olxp;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.olxpbenchmark.api.Procedure;
import com.olxpbenchmark.api.SQLStmt;
import com.olxpbenchmark.benchmarks.tabenchmark.TAConstants;

public class X2 extends Procedure {

    public final SQLStmt getAccessInfo = new SQLStmt(
            "SELECT data1, data2, data3, data4 FROM " + TAConstants.TABLENAME_ACCESS_INFO +
                    " WHERE s_id = ? AND ai_type = ?"
    );

    public final SQLStmt stmt_query = new SQLStmt(
            "SELECT MIN(ai_type) FROM " + TAConstants.TABLENAME_ACCESS_INFO
    );

    public void run(Connection conn, long s_id, byte ai_type) throws SQLException {
        PreparedStatement stmt = this.getPreparedStatement(conn, getAccessInfo);
        stmt.setLong(1, s_id);
        stmt.setByte(2, ai_type);
        ResultSet results = stmt.executeQuery();
        assert(results != null);
        results.close();

        stmt = this.getPreparedStatement(conn, stmt_query);
        ResultSet results2 = stmt.executeQuery();
        assert(results2 != null);
        results2.close();


    }
}


