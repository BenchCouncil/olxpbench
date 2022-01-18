package com.olxpbenchmark.benchmarks.subenchmark.procedures.oltp;

import com.olxpbenchmark.api.SQLStmt;
import com.olxpbenchmark.benchmarks.subenchmark.procedures.oltp.OLTPConstants;
import com.olxpbenchmark.benchmarks.subenchmark.procedures.oltp.OLTPUtil;
import com.olxpbenchmark.benchmarks.subenchmark.SUOltpWroker;
import com.olxpbenchmark.benchmarks.subenchmark.pojo.Customer;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.ArrayList;
import java.util.Random;

public class OrderStatus extends OLTPProcedure {

    private static final Logger LOG = Logger.getLogger(OrderStatus.class);

    public SQLStmt ordStatGetNewestOrdSQL = new SQLStmt(
            "SELECT O_ID, O_CARRIER_ID, O_ENTRY_D " +
                    "  FROM " + OLTPConstants.TABLENAME_OPENORDER +
                    " WHERE O_W_ID = ? " +
                    "   AND O_D_ID = ? " +
                    "   AND O_C_ID = ? " +
                    " ORDER BY O_ID DESC LIMIT 1");

    public SQLStmt ordStatGetOrderLinesSQL = new SQLStmt(
            "SELECT OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D " +
                    "  FROM " + OLTPConstants.TABLENAME_ORDERLINE +
                    " WHERE OL_O_ID = ?" +
                    "   AND OL_D_ID = ?" +
                    "   AND OL_W_ID = ?");

    public SQLStmt payGetCustSQL = new SQLStmt(
            "SELECT C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, " +
                    "       C_CITY, C_STATE, C_ZIP, C_PHONE, C_CREDIT, C_CREDIT_LIM, " +
                    "       C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_SINCE " +
                    "  FROM " + OLTPConstants.TABLENAME_CUSTOMER +
                    " WHERE C_W_ID = ? " +
                    "   AND C_D_ID = ? " +
                    "   AND C_ID = ?");

    public SQLStmt customerByNameSQL = new SQLStmt(
            "SELECT C_FIRST, C_MIDDLE, C_ID, C_STREET_1, C_STREET_2, C_CITY, " +
                    "       C_STATE, C_ZIP, C_PHONE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, " +
                    "       C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_SINCE " +
                    "  FROM " + OLTPConstants.TABLENAME_CUSTOMER +
                    " WHERE C_W_ID = ? " +
                    "   AND C_D_ID = ? " +
                    "   AND C_LAST = ? " +
                    " ORDER BY C_FIRST");

    private PreparedStatement ordStatGetNewestOrd = null;
    private PreparedStatement ordStatGetOrderLines = null;
    private PreparedStatement payGetCust = null;
    private PreparedStatement customerByName = null;


    public ResultSet run(Connection conn, Random gen, int w_id, int numWarehouses, int terminalDistrictLowerID, int terminalDistrictUpperID, SUOltpWroker w) throws SQLException {
        boolean trace = LOG.isTraceEnabled();

        // initializing all prepared statements
        payGetCust = this.getPreparedStatement(conn, payGetCustSQL);
        customerByName = this.getPreparedStatement(conn, customerByNameSQL);
        ordStatGetNewestOrd = this.getPreparedStatement(conn, ordStatGetNewestOrdSQL);
        ordStatGetOrderLines = this.getPreparedStatement(conn, ordStatGetOrderLinesSQL);

        int d_id = OLTPUtil.randomNumber(terminalDistrictLowerID, terminalDistrictUpperID, gen);
        boolean c_by_name = false;
        int y = OLTPUtil.randomNumber(1, 100, gen);
        String c_last = null;
        int c_id = -1;
        if (y <= 60) {
            c_by_name = true;
            c_last = OLTPUtil.getNonUniformRandomLastNameForRun(gen);
        } else {
            c_by_name = false;
            c_id = OLTPUtil.getCustomerID(gen);
        }

        int o_id = -1, o_carrier_id = -1;
        Timestamp o_entry_d;
        ArrayList<String> orderLines = new ArrayList<String>();

        Customer c;
        if (c_by_name) {
            assert c_id <= 0;
            // TODO: This only needs c_balance, c_first, c_middle, c_id
            // only fetch those columns?
            c = getCustomerByName(w_id, d_id, c_last);
        } else {
            assert c_last == null;
            c = getCustomerById(w_id, d_id, c_id, conn);
        }

        // find the newest order for the customer
        // retrieve the carrier & order date for the most recent order.

        ordStatGetNewestOrd.setInt(1, w_id);
        ordStatGetNewestOrd.setInt(2, d_id);
        ordStatGetNewestOrd.setInt(3, c.c_id);
        if (trace) LOG.trace("ordStatGetNewestOrd START");
        ResultSet rs = ordStatGetNewestOrd.executeQuery();
        if (trace) LOG.trace("ordStatGetNewestOrd END");

        if (!rs.next()) {
            String msg = String.format("No order records for CUSTOMER [C_W_ID=%d, C_D_ID=%d, C_ID=%d]",
                    w_id, d_id, c.c_id);
            if (trace) LOG.warn(msg);
            throw new RuntimeException(msg);
        }

        o_id = rs.getInt("O_ID");
        o_carrier_id = rs.getInt("O_CARRIER_ID");
        o_entry_d = rs.getTimestamp("O_ENTRY_D");
        rs.close();

        // retrieve the order lines for the most recent order
        ordStatGetOrderLines.setInt(1, o_id);
        ordStatGetOrderLines.setInt(2, d_id);
        ordStatGetOrderLines.setInt(3, w_id);
        if (trace) LOG.trace("ordStatGetOrderLines START");
        rs = ordStatGetOrderLines.executeQuery();
        if (trace) LOG.trace("ordStatGetOrderLines END");

        while (rs.next()) {
            StringBuilder sb = new StringBuilder();
            sb.append("[");
            sb.append(rs.getLong("OL_SUPPLY_W_ID"));
            sb.append(" - ");
            sb.append(rs.getLong("OL_I_ID"));
            sb.append(" - ");
            sb.append(rs.getLong("OL_QUANTITY"));
            sb.append(" - ");
            sb.append(OLTPUtil.formattedDouble(rs.getDouble("OL_AMOUNT")));
            sb.append(" - ");
            if (rs.getTimestamp("OL_DELIVERY_D") != null)
                sb.append(rs.getTimestamp("OL_DELIVERY_D"));
            else
                sb.append("99-99-9999");
            sb.append("]");
            orderLines.add(sb.toString());
        }
        rs.close();
        rs = null;

        // commit the transaction
        conn.commit();

        if (orderLines.isEmpty()) {
            String msg = String.format("Order record had no order line items [C_W_ID=%d, C_D_ID=%d, C_ID=%d, O_ID=%d]",
                    w_id, d_id, c.c_id, o_id);
            if (trace) LOG.warn(msg);
        }

        if (trace) {
            StringBuilder sb = new StringBuilder();
            sb.append("\n");
            sb.append("+-------------------------- ORDER-STATUS -------------------------+\n");
            sb.append(" Date: ");
            sb.append(OLTPUtil.getCurrentTime());
            sb.append("\n\n Warehouse: ");
            sb.append(w_id);
            sb.append("\n District:  ");
            sb.append(d_id);
            sb.append("\n\n Customer:  ");
            sb.append(c.c_id);
            sb.append("\n   Name:    ");
            sb.append(c.c_first);
            sb.append(" ");
            sb.append(c.c_middle);
            sb.append(" ");
            sb.append(c.c_last);
            sb.append("\n   Balance: ");
            sb.append(c.c_balance);
            sb.append("\n\n");
            if (o_id == -1) {
                sb.append(" Customer has no orders placed.\n");
            } else {
                sb.append(" Order-Number: ");
                sb.append(o_id);
                sb.append("\n    Entry-Date: ");
                sb.append(o_entry_d);
                sb.append("\n    Carrier-Number: ");
                sb.append(o_carrier_id);
                sb.append("\n\n");
                if (orderLines.size() != 0) {
                    sb.append(" [Supply_W - Item_ID - Qty - Amount - Delivery-Date]\n");
                    for (String orderLine : orderLines) {
                        sb.append(" ");
                        sb.append(orderLine);
                        sb.append("\n");
                    }
                } else {
                    LOG.trace(" This Order has no Order-Lines.\n");
                }
            }
            sb.append("+-----------------------------------------------------------------+\n\n");
            LOG.trace(sb.toString());
        }

        return null;
    }

    // attention duplicated code across trans... ok for now to maintain separate
    // prepared statements
    public Customer getCustomerById(int c_w_id, int c_d_id, int c_id, Connection conn) throws SQLException {
        boolean trace = LOG.isTraceEnabled();

        payGetCust.setInt(1, c_w_id);
        payGetCust.setInt(2, c_d_id);
        payGetCust.setInt(3, c_id);
        if (trace) LOG.trace("payGetCust START");
        ResultSet rs = payGetCust.executeQuery();
        if (trace) LOG.trace("payGetCust END");
        if (!rs.next()) {
            String msg = String.format("Failed to get CUSTOMER [C_W_ID=%d, C_D_ID=%d, C_ID=%d]",
                    c_w_id, c_d_id, c_id);
            if (trace) LOG.warn(msg);
            throw new RuntimeException(msg);
        }

        Customer c = OLTPUtil.newCustomerFromResults(rs);
        c.c_id = c_id;
        c.c_last = rs.getString("C_LAST");
        rs.close();
        return c;
    }

    // attention this code is repeated in other transacitons... ok for now to
    // allow for separate statements.
    public Customer getCustomerByName(int c_w_id, int c_d_id, String c_last) throws SQLException {
        ArrayList<Customer> customers = new ArrayList<Customer>();
        boolean trace = LOG.isDebugEnabled();

        customerByName.setInt(1, c_w_id);
        customerByName.setInt(2, c_d_id);
        customerByName.setString(3, c_last);
        if (trace) LOG.trace("customerByName START");
        ResultSet rs = customerByName.executeQuery();
        if (trace) LOG.trace("customerByName END");

        while (rs.next()) {
            Customer c = OLTPUtil.newCustomerFromResults(rs);
            c.c_id = rs.getInt("C_ID");
            c.c_last = c_last;
            customers.add(c);
        }
        rs.close();

        if (customers.size() == 0) {
            String msg = String.format("Failed to get CUSTOMER [C_W_ID=%d, C_D_ID=%d, C_LAST=%s]",
                    c_w_id, c_d_id, c_last);
            if (trace) LOG.warn(msg);
            throw new RuntimeException(msg);
        }

        // TPC-C 2.5.2.2: Position n / 2 rounded up to the next integer, but
        // that counts starting from 1.
        int index = customers.size() / 2;
        if (customers.size() % 2 == 0) {
            index -= 1;
        }
        return customers.get(index);
    }



}
