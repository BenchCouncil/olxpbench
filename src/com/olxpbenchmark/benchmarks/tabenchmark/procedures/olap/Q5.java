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

public class Q5 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt(
            "  SELECT (100.00 * sum(CASE WHEN ACCESS_INFO.data3 LIKE 'A%' THEN ACCESS_INFO.data1 ELSE 0 END) / (1 + sum(CALL_FORWARDING.sf_type))) AS promo_revenue "
                    + "from ACCESS_INFO, CALL_FORWARDING "
                    + " WHERE  ACCESS_INFO.s_id = CALL_FORWARDING.s_id "
                    + "AND CALL_FORWARDING.start_time >= 4 "
                    + "AND CALL_FORWARDING.end_time < 16 "
    );

    @Override
    protected PreparedStatement getStatement(Connection conn, RandomGenerator rand) throws SQLException {

        PreparedStatement stmt = this.getPreparedStatement(conn, query_stmt);

        return stmt;
    }
}





