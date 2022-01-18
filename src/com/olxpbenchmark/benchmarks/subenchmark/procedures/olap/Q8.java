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

 public class Q8 extends GenericQuery {

     public final SQLStmt query_stmt = new SQLStmt(
             "select I_NAME, "
                     + "substring(I_DATA from  1 for 3) AS brand, "
                     + "I_PRICE,  "
                     + "count(DISTINCT (mod((S_W_ID * S_I_ID),10000))) AS supplier_cnt "
                     + "from STOCK, ITEM "
                     + " WHERE I_ID = S_I_ID "
                     + " AND I_DATA NOT LIKE 'zz%' "
                     + " AND (mod((S_W_ID * S_I_ID), 10000) NOT IN "
                     + " (SELECT H_C_ID "
                     + " FROM HISTORY "
                     + " WHERE H_DATA LIKE '%l%')) "
                     + " GROUP BY I_NAME, brand, I_PRICE "
                     + " ORDER BY supplier_cnt DESC "
     );

     @Override
     protected PreparedStatement getStatement(Connection conn, RandomGenerator rand) throws SQLException {
         PreparedStatement stmt = this.getPreparedStatement(conn, query_stmt);
         // Fix it!
         //stmt.setString(1, delta);
         return stmt;
     }
 }


