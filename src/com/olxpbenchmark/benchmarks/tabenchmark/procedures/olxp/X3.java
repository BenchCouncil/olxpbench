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

public class X3 extends Procedure {

    public final SQLStmt getNewDestination = new SQLStmt(
            "SELECT cf.numberx " +
                    "  FROM " + TAConstants.TABLENAME_SPECIAL_FACILITY + " sf, " +
                    "       " + TAConstants.TABLENAME_CALL_FORWARDING + " cf " +
                    " WHERE sf.s_id = ? " +
                    "   AND sf.sf_type = ? " +
                    "   AND sf.is_active = 1 " +
                    "   AND cf.s_id = sf.s_id " +
                    "   AND cf.sf_type = sf.sf_type " +
                    "   AND cf.start_time <= ? " +
                    "   AND cf.end_time > ?"
    );

    public final SQLStmt stmt_query = new SQLStmt(
            "SELECT AVG(sf_type) FROM " + TAConstants.TABLENAME_SPECIAL_FACILITY
    );

    public void run(Connection conn, long s_id, byte sf_type, byte start_time, byte end_time) throws SQLException {
        PreparedStatement stmt = this.getPreparedStatement(conn, getNewDestination);
        stmt.setLong(1, s_id);
        stmt.setByte(2, sf_type);
        stmt.setByte(3, start_time);
        stmt.setByte(4, end_time);
        ResultSet results = stmt.executeQuery();
        assert(results != null);
        results.close();

        stmt = this.getPreparedStatement(conn, stmt_query);
        ResultSet results2 = stmt.executeQuery();
        assert(results2 != null);
        results2.close();

    }
}

