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

 public class Q10 extends GenericQuery {

     public final SQLStmt query_stmt = new SQLStmt(
             "SELECT substring(C_STATE from 1 for 1) AS country, count(*) AS numcust,  sum(C_BALANCE) AS totacctbal "
                     + "FROM CUSTOMER "
                     + "WHERE substring(C_PHONE from 1 for 1) IN ('1', '2', '3', '4', '5', '6', '7') "
                     + "AND C_BALANCE > "
                     + "(SELECT avg(C_BALANCE) "
                     + "FROM CUSTOMER "
                     + "WHERE C_BALANCE > 0.00 "
                     + "AND substring(C_PHONE from 1 for 1) IN ('1', '2', '3', '4', '5', '6', '7')) "
             + " AND NOT EXISTS "
             + " (SELECT * "
             + "FROM OORDER "
             + "WHERE O_C_ID = C_ID "
             + "AND O_W_ID=C_W_ID "
             + "AND O_D_ID=C_D_ID) "
             + "GROUP BY substring(C_STATE from 1 for 1) "
             + "ORDER BY substring(C_STATE,1,1) "
     );

     @Override
     protected PreparedStatement getStatement(Connection conn, RandomGenerator rand) throws SQLException {
         PreparedStatement stmt = this.getPreparedStatement(conn, query_stmt);
         // Fix it!
         //stmt.setString(1, delta);
         return stmt;
     }
 }



