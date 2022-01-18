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

package com.olxpbenchmark.benchmarks.subenchmark.procedures.olap;

import com.olxpbenchmark.api.SQLStmt;
import com.olxpbenchmark.benchmarks.subenchmark.procedures.olap.GenericQuery;
import com.olxpbenchmark.util.RandomGenerator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Q6 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt(
            "select C_COUNT, "
                    + "count(*) AS custdist "
                    + "from  "
                       + "(select C_ID, count(O_ID) AS C_COUNT "
                       + "FROM CUSTOMER "
                       + "LEFT OUTER JOIN OORDER ON (C_W_ID = O_W_ID "
                       + "AND C_D_ID=O_D_ID "
                       + "AND C_ID=O_C_ID "
                       + "AND O_CARRIER_ID>8) "
                       + "GROUP BY C_ID) AS C_ORDERS "
                    + "GROUP BY C_COUNT "
                    + "ORDER BY custdist DESC, C_COUNT DESC"
    );

    @Override
    protected PreparedStatement getStatement(Connection conn, RandomGenerator rand) throws SQLException {
        PreparedStatement stmt = this.getPreparedStatement(conn, query_stmt);
        // Fix it!
        //stmt.setString(1, delta);
        return stmt;
    }
}



