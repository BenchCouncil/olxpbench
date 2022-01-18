/******************************************************************************
 *  Copyright 2015 by OLTPBenchmark Project                                   *
 *                                                                            *
 *  Licensed under the Apache License, Version 2.0 (the "License");           *
 *  you may not use this file except in compliance with the License.          *
 *  You may obtain a copy of the License at                                   *
 *                                                                            *
 *    http://www.apache.org/licenses/LICENSE-2.0                              *
 *                                                                            *
 *  Unless required by applicable law or agreed to in writing, software       *
 *  distributed under the License is distributed on an "AS IS" BASIS,         *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 *  See the License for the specific language governing permissions and       *
 *  limitations under the License.                                            *
 ******************************************************************************/


package com.olxpbenchmark.benchmarks.tabenchmark.procedures.oltp;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.olxpbenchmark.api.Procedure;
import com.olxpbenchmark.api.SQLStmt;
import com.olxpbenchmark.benchmarks.tabenchmark.TAConstants;

public class InsertCallForwarding extends Procedure {

    public final SQLStmt getSubscriber = new SQLStmt(
            "SELECT s_id FROM " + TAConstants.TABLENAME_SUBSCRIBER + " WHERE sub_nbr = ?"
    );

    public final SQLStmt getSpecialFacility = new SQLStmt(
            "SELECT sf_type FROM " + TAConstants.TABLENAME_SPECIAL_FACILITY + " WHERE s_id = ?"
    );

    public final SQLStmt insertCallForwarding = new SQLStmt(
            "INSERT INTO " + TAConstants.TABLENAME_CALL_FORWARDING + " VALUES (?, ?, ?, ?, ?)"
    );

    public long run(Connection conn, String sub_nbr, byte sf_type, byte start_time, byte end_time, String numberx) throws SQLException {
        PreparedStatement stmt = this.getPreparedStatement(conn, getSubscriber);
        stmt.setString(1, sub_nbr);
        ResultSet results = stmt.executeQuery();
        assert(results != null);
        long s_id=-1;
        if(results.next())
        {
            s_id = results.getLong(1);
        }
        assert s_id!=-1;
        results.close();
        stmt = this.getPreparedStatement(conn, getSpecialFacility);
        stmt.setLong(1, s_id);
        ResultSet results2 = stmt.executeQuery();
        assert(results2 != null);
        results2.close();

        // Inserting a new CALL_FORWARDING record only succeeds 30% of the time
        stmt = this.getPreparedStatement(conn, insertCallForwarding);
        stmt.setLong(1, s_id);
        stmt.setByte(2, sf_type);
        stmt.setByte(3, start_time);
        stmt.setByte(4, end_time);
        stmt.setString(5, numberx);
        int rows_updated = -1;

        try {
            rows_updated = stmt.executeUpdate();
        } catch (SQLException ex) {
            throw new UserAbortException("Failed to insert a row in " + TAConstants.TABLENAME_CALL_FORWARDING);
        }
        return (rows_updated);
    }
}
