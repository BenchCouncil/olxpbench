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

public class Q5 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt(
            "select O_OL_CNT, "
                    + "sum(CASE WHEN O_CARRIER_ID=1  "
                    + "OR   O_CARRIER_ID =  2 THEN 1 ELSE 0 END) AS high_line_count, "
                    + "sum(CASE WHEN O_CARRIER_ID <> 1 "
                    + "AND  O_CARRIER_ID <> 2 THEN 1 ELSE 0 END) AS low_line_count "
                    + "from OORDER, "
                    + "ORDER_LINE "
                    + " WHERE OL_W_ID = O_W_ID "
                    + "AND OL_D_ID = O_D_ID "
                    + "AND OL_O_ID = O_ID "
                    + "AND O_ENTRY_D <= OL_DELIVERY_D "
                    + "AND OL_DELIVERY_D < '2020-01-01 00:00:00.000000' "
                    + "GROUP BY O_OL_CNT "
                    + "ORDER BY O_OL_CNT "
    );

    @Override
    protected PreparedStatement getStatement(Connection conn, RandomGenerator rand) throws SQLException {
        PreparedStatement stmt = this.getPreparedStatement(conn, query_stmt);
        // Fix it!
        //stmt.setString(1, delta);
        return stmt;
    }
}


