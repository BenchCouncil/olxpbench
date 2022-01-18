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

package com.olxpbenchmark.benchmarks.subenchmark.procedures.olxp;

import com.olxpbenchmark.api.SQLStmt;
import com.olxpbenchmark.benchmarks.subenchmark.procedures.oltp.OLTPConstants;
import com.olxpbenchmark.benchmarks.subenchmark.procedures.oltp.OLTPProcedure;
import com.olxpbenchmark.benchmarks.subenchmark.procedures.oltp.OLTPUtil;
import com.olxpbenchmark.benchmarks.subenchmark.SUOltpWroker;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Random;

public class X5  extends OLTPProcedure {

    private static final Logger LOG = Logger.getLogger(X5.class);

    public SQLStmt stockGetDistOrderIdSQL = new SQLStmt(
            "SELECT D_NEXT_O_ID " +
                    "  FROM " + OLTPConstants.TABLENAME_DISTRICT +
                    " WHERE D_W_ID = ? " +
                    "   AND D_ID = ?");

    public SQLStmt stockGetCountStockSQL = new SQLStmt(
            "SELECT COUNT(DISTINCT (S_I_ID)) AS STOCK_COUNT " +
                    " FROM " + OLTPConstants.TABLENAME_ORDERLINE + ", " + OLTPConstants.TABLENAME_STOCK +
                    " WHERE OL_W_ID = ?" +
                    " AND OL_D_ID = ?" +
                    " AND OL_O_ID < ?" +
                    " AND OL_O_ID >= ?" +
                    " AND S_W_ID = ?" +
                    " AND S_I_ID = OL_I_ID" +
                    " AND S_QUANTITY < ?");

    public SQLStmt stockGetMaxStockSQL = new SQLStmt(
            "SELECT MAX(S_DATA) " +
                    " FROM "  + OLTPConstants.TABLENAME_STOCK );

    // Stock Level Txn
    private PreparedStatement stockGetDistOrderId = null;
    private PreparedStatement stockGetCountStock = null;
    private PreparedStatement stockGetMaxStock =null;

    public ResultSet run(Connection conn, Random gen,
                         int w_id, int numWarehouses,
                         int terminalDistrictLowerID, int terminalDistrictUpperID,
                         SUOltpWroker w) throws SQLException {

        boolean trace = LOG.isTraceEnabled();

        stockGetDistOrderId = this.getPreparedStatement(conn, stockGetDistOrderIdSQL);
        stockGetCountStock= this.getPreparedStatement(conn, stockGetCountStockSQL);
        stockGetMaxStock = this.getPreparedStatement(conn, stockGetMaxStockSQL);

        int threshold = OLTPUtil.randomNumber(10, 20, gen);
        int d_id = OLTPUtil.randomNumber(terminalDistrictLowerID,terminalDistrictUpperID, gen);

        int o_id = 0;
        // XXX int i_id = 0;
        int stock_count = 0;

        stockGetDistOrderId.setInt(1, w_id);
        stockGetDistOrderId.setInt(2, d_id);
        if (trace) LOG.trace(String.format("stockGetDistOrderId BEGIN [W_ID=%d, D_ID=%d]", w_id, d_id));
        ResultSet rs = stockGetDistOrderId.executeQuery();
        if (trace) LOG.trace("stockGetDistOrderId END");

        if (!rs.next()) {
            throw new RuntimeException("D_W_ID="+ w_id +" D_ID="+ d_id+" not found!");
        }
        o_id = rs.getInt("D_NEXT_O_ID");
        rs.close();

        stockGetCountStock.setInt(1, w_id);
        stockGetCountStock.setInt(2, d_id);
        stockGetCountStock.setInt(3, o_id);
        stockGetCountStock.setInt(4, o_id - 20);
        stockGetCountStock.setInt(5, w_id);
        stockGetCountStock.setInt(6, threshold);
        if (trace) LOG.trace(String.format("stockGetCountStock BEGIN [W_ID=%d, D_ID=%d, O_ID=%d]", w_id, d_id, o_id));
        rs = stockGetCountStock.executeQuery();
        if (trace) LOG.trace("stockGetCountStock END");

        if (!rs.next()) {
            String msg = String.format("Failed to get StockLevel result for COUNT query " +
                    "[W_ID=%d, D_ID=%d, O_ID=%d]", w_id, d_id, o_id);
            if (trace) LOG.warn(msg);
            throw new RuntimeException(msg);
        }
        stock_count = rs.getInt("STOCK_COUNT");
        if (trace) LOG.trace("stockGetCountStock RESULT=" + stock_count);

        conn.commit();
        rs.close();

        rs = stockGetMaxStock.executeQuery();
        conn.commit();
        rs.close();

        if (trace) {
            StringBuilder terminalMessage = new StringBuilder();
            terminalMessage.append("\n+-------------------------- STOCK-LEVEL --------------------------+");
            terminalMessage.append("\n Warehouse: ");
            terminalMessage.append(w_id);
            terminalMessage.append("\n District:  ");
            terminalMessage.append(d_id);
            terminalMessage.append("\n\n Stock Level Threshold: ");
            terminalMessage.append(threshold);
            terminalMessage.append("\n Low Stock Count:       ");
            terminalMessage.append(stock_count);
            terminalMessage.append("\n+-----------------------------------------------------------------+\n\n");
            LOG.trace(terminalMessage.toString());
        }
        return null;
    }
}

