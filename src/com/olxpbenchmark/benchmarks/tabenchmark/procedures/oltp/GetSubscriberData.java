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

public class GetSubscriberData extends Procedure {

    public final SQLStmt getSubscriber = new SQLStmt(
            "SELECT * FROM " + TAConstants.TABLENAME_SUBSCRIBER + " WHERE s_id = ?"
    );

    public void run(Connection conn, long s_id) throws SQLException {
        PreparedStatement stmt = this.getPreparedStatement(conn, getSubscriber);
        stmt.setLong(1, s_id);
        ResultSet results = stmt.executeQuery();
        assert(results != null);
        results.close();
    }
}

