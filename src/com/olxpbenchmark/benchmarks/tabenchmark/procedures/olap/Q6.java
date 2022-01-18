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
import com.olxpbenchmark.benchmarks.subenchmark.procedures.oltp.OLTPConstants;
import com.olxpbenchmark.benchmarks.tabenchmark.TAConstants;
import com.olxpbenchmark.util.RandomGenerator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Q6 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt(
            "select ACCESS_INFO.ai_type, "
                    + "substring(ACCESS_INFO.data3 from  1 for 3) AS brand, "
                    + "SUBSCRIBER.sub_nbr "
                    + "from ACCESS_INFO, SUBSCRIBER "
                    + " WHERE ACCESS_INFO.s_id = SUBSCRIBER.s_id "
                    + " AND SUBSCRIBER.sub_nbr NOT LIKE 'zz%' "
                    + " GROUP BY ACCESS_INFO.ai_type, SUBSCRIBER.sub_nbr "
                    + " ORDER BY SUBSCRIBER.sub_nbr DESC "
    );


    @Override
    protected PreparedStatement getStatement(Connection conn, RandomGenerator rand) throws SQLException {

        PreparedStatement stmt = this.getPreparedStatement(conn, query_stmt);

        return stmt;
    }
}






