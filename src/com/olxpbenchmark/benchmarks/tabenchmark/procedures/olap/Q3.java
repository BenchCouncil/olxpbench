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

package com.olxpbenchmark.benchmarks.tabenchmark.procedures.olap;

import com.olxpbenchmark.api.SQLStmt;
import com.olxpbenchmark.util.RandomGenerator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Q3 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt(
            "select avg(CALL_FORWARDING.start_time) AS avg_start "
                    + "from CALL_FORWARDING "
                    + "WHERE CALL_FORWARDING.numberx LIKE '%00%' "
                    + "AND CALL_FORWARDING.start_time BETWEEN 1 AND 40"
    );

    @Override
    protected PreparedStatement getStatement(Connection conn, RandomGenerator rand) throws SQLException {

        PreparedStatement stmt = this.getPreparedStatement(conn, query_stmt);

        return stmt;
    }
}




