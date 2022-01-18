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

package com.olxpbenchmark.benchmarks.fibenchmark.procedures.olap;

import com.olxpbenchmark.api.SQLStmt;
import com.olxpbenchmark.util.RandomGenerator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Q2 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt(
            "select CHECKING.custid, sum(SAVINGS.bal) AS sa_bal "
                    + " from "
                    + "CHECKING, "
                    + " SAVINGS "
                    + "where "
                    + "SAVINGS.custid=CHECKING.custid "
                    + "and CHECKING.bal >= ?  "
                    + "group by CHECKING.custid "
                    + "order by sa_bal DESC"
    );

    @Override
    protected PreparedStatement getStatement(Connection conn, RandomGenerator rand) throws SQLException {
        int cbal = rand.number(30000, 40000);

        PreparedStatement stmt = this.getPreparedStatement(conn, query_stmt);
        // Fix it!
        stmt.setInt(1, cbal);

        return stmt;
    }
}


