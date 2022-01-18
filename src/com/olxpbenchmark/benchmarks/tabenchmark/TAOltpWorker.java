/*
 * Copyright 2021 OLxPBench
 * This work was based on the OLTPBenchmark Project

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

package com.olxpbenchmark.benchmarks.tabenchmark;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import com.olxpbenchmark.benchmarks.tabenchmark.procedures.olxp.X1;
import org.apache.log4j.Logger;

import com.olxpbenchmark.api.Procedure;
import com.olxpbenchmark.api.TransactionType;
import com.olxpbenchmark.api.Worker;
import com.olxpbenchmark.api.Procedure.UserAbortException;
import com.olxpbenchmark.benchmarks.tabenchmark.procedures.oltp.DeleteCallForwarding;
import com.olxpbenchmark.benchmarks.tabenchmark.procedures.oltp.GetAccessData;
import com.olxpbenchmark.benchmarks.tabenchmark.procedures.oltp.GetNewDestination;
import com.olxpbenchmark.benchmarks.tabenchmark.procedures.oltp.GetSubscriberData;
import com.olxpbenchmark.benchmarks.tabenchmark.procedures.oltp.InsertCallForwarding;
import com.olxpbenchmark.benchmarks.tabenchmark.procedures.oltp.UpdateLocation;
import com.olxpbenchmark.benchmarks.tabenchmark.procedures.oltp.UpdateSubscriberData;
import com.olxpbenchmark.benchmarks.tabenchmark.procedures.olxp.X1;
import com.olxpbenchmark.benchmarks.tabenchmark.procedures.olxp.X2;
import com.olxpbenchmark.benchmarks.tabenchmark.procedures.olxp.X3;
import com.olxpbenchmark.benchmarks.tabenchmark.procedures.olxp.X4;
import com.olxpbenchmark.benchmarks.tabenchmark.procedures.olxp.X5;
import com.olxpbenchmark.benchmarks.tabenchmark.procedures.olxp.X6;
import com.olxpbenchmark.types.TransactionStatus;

public class TAOltpWorker extends Worker<TABenchmark> {
    private static final Logger LOG = Logger.getLogger(TAOltpWorker.class);

    /**
     * Each Transaction element provides an TransactionInvoker to create the proper
     * arguments used to invoke the stored procedure
     */
    private static interface TransactionInvoker<T extends Procedure> {
        /**
         * Generate the proper arguments used to invoke the given stored procedure
         * @param subscriberSize
         * @return
         */
        public void invoke(Connection conn, Procedure proc, long subscriberSize) throws SQLException;
    }

    /**
     * Set of transactions structs with their appropriate parameters
     */
    public static enum Transaction {
        X1(new TransactionInvoker<X1>() {
            public void invoke(Connection conn, Procedure proc, long subscriberSize) throws SQLException {
                long s_id = TAUtil.getSubscriberId(subscriberSize);
                ((X1)proc).run(
                        conn,
                        TAUtil.padWithZero(s_id), // s_id
                        TAUtil.number(1, 4).byteValue(), // sf_type
                        (byte)(8 * TAUtil.number(0, 2)) // start_time
                );
            }
        }),
        DeleteCallForwarding(new TransactionInvoker<DeleteCallForwarding>() {
            public void invoke(Connection conn, Procedure proc, long subscriberSize) throws SQLException {
                long s_id = TAUtil.getSubscriberId(subscriberSize);
                ((DeleteCallForwarding)proc).run(
                        conn,
                        TAUtil.padWithZero(s_id), // s_id
                        TAUtil.number(1, 4).byteValue(), // sf_type
                        (byte)(8 * TAUtil.number(0, 2)) // start_time
                );
            }
        }),
        X2(new TransactionInvoker<X2>() {
            public void invoke(Connection conn, Procedure proc, long subscriberSize) throws SQLException {
                long s_id = TAUtil.getSubscriberId(subscriberSize);
                ((X2)proc).run(
                        conn,
                        s_id, // s_id
                        TAUtil.number(1, 4).byteValue() // ai_type
                );
            }
        }),
        GetAccessData(new TransactionInvoker<GetAccessData>() {
            public void invoke(Connection conn, Procedure proc, long subscriberSize) throws SQLException {
                long s_id = TAUtil.getSubscriberId(subscriberSize);
                ((GetAccessData)proc).run(
                        conn,
                        s_id, // s_id
                        TAUtil.number(1, 4).byteValue() // ai_type
                );
            }
        }),
        X3(new TransactionInvoker<X3>() {
            public void invoke(Connection conn, Procedure proc, long subscriberSize) throws SQLException {
                long s_id = TAUtil.getSubscriberId(subscriberSize);
                ((X3)proc).run(
                        conn,
                        s_id, // s_id
                        TAUtil.number(1, 4).byteValue(), // sf_type
                        (byte)(8 * TAUtil.number(0, 2)), // start_time
                        TAUtil.number(1, 24).byteValue() // end_time
                );
            }
        }),
        GetNewDestination(new TransactionInvoker<GetNewDestination>() {
            public void invoke(Connection conn, Procedure proc, long subscriberSize) throws SQLException {
                long s_id = TAUtil.getSubscriberId(subscriberSize);
                ((GetNewDestination)proc).run(
                        conn,
                        s_id, // s_id
                        TAUtil.number(1, 4).byteValue(), // sf_type
                        (byte)(8 * TAUtil.number(0, 2)), // start_time
                        TAUtil.number(1, 24).byteValue() // end_time
                );
            }
        }),
        X5(new TransactionInvoker<X5>() {
            public void invoke(Connection conn, Procedure proc, long subscriberSize) throws SQLException {
                long s_id = TAUtil.getSubscriberId(subscriberSize);
                ((X5)proc).run(
                        conn,
                        s_id // s_id
                );
            }
        }),
        X6(new TransactionInvoker<X6>() {
            public void invoke(Connection conn, Procedure proc, long subscriberSize) throws SQLException {
                long s_id = TAUtil.getSubscriberId(subscriberSize);
                ((X6)proc).run(
                        conn,
                        s_id // s_id
                );
            }
        }),
        GetSubscriberData(new TransactionInvoker<GetSubscriberData>() {
            public void invoke(Connection conn, Procedure proc, long subscriberSize) throws SQLException {
                long s_id = TAUtil.getSubscriberId(subscriberSize);
                ((GetSubscriberData)proc).run(
                        conn,
                        s_id // s_id
                );
            }
        }),
        X4(new TransactionInvoker<X4>() {
            public void invoke(Connection conn, Procedure proc, long subscriberSize) throws SQLException {
                long s_id = TAUtil.getSubscriberId(subscriberSize);
                ((X4)proc).run(
                        conn,
                        TAUtil.padWithZero(s_id), // sub_nbr
                        TAUtil.number(1, 4).byteValue(), // sf_type
                        (byte)(8 * TAUtil.number(0, 2)), // start_time
                        TAUtil.number(1, 24).byteValue(), // end_time
                        TAUtil.padWithZero(s_id) // numberx
                );
            }
        }),
        InsertCallForwarding(new TransactionInvoker<InsertCallForwarding>() {
            public void invoke(Connection conn, Procedure proc, long subscriberSize) throws SQLException {
                long s_id = TAUtil.getSubscriberId(subscriberSize);
                ((InsertCallForwarding)proc).run(
                        conn,
                        TAUtil.padWithZero(s_id), // sub_nbr
                        TAUtil.number(1, 4).byteValue(), // sf_type
                        (byte)(8 * TAUtil.number(0, 2)), // start_time
                        TAUtil.number(1, 24).byteValue(), // end_time
                        TAUtil.padWithZero(s_id) // numberx
                );
            }
        }),
        UpdateLocation(new TransactionInvoker<UpdateLocation>() {
            public void invoke(Connection conn, Procedure proc, long subscriberSize) throws SQLException {
                long s_id = TAUtil.getSubscriberId(subscriberSize);
                ((UpdateLocation)proc).run(
                        conn,
                        TAUtil.number(0, Integer.MAX_VALUE).intValue(), // vlr_location
                        TAUtil.padWithZero(s_id) // sub_nbr
                );
            }
        }),
        UpdateSubscriberData(new TransactionInvoker<UpdateSubscriberData>() {
            public void invoke(Connection conn, Procedure proc, long subscriberSize) throws SQLException {
                long s_id = TAUtil.getSubscriberId(subscriberSize);
                ((UpdateSubscriberData)proc).run(
                        conn,
                        s_id, // s_id
                        TAUtil.number(0, 1).byteValue(), // bit_1
                        TAUtil.number(0, 255).shortValue(), // data_a
                        TAUtil.number(1, 4).byteValue() // sf_type
                );
            }
        }),
        ; // END LIST OF STORED PROCEDURES

        /**
         * Constructor
         */
        private Transaction(TransactionInvoker<? extends Procedure> ag) {
            this.generator = ag;
        }

        public final TransactionInvoker<? extends Procedure> generator;

        protected static final Map<Integer, Transaction> idx_lookup = new HashMap<Integer, Transaction>();
        protected static final Map<String, Transaction> name_lookup = new HashMap<String, Transaction>();
        static {
            for (Transaction vt : EnumSet.allOf(Transaction.class)) {
                Transaction.idx_lookup.put(vt.ordinal(), vt);
                Transaction.name_lookup.put(vt.name().toUpperCase(), vt);
            }
        }

        public static Transaction get(String name) {
            Transaction ret = Transaction.name_lookup.get(name.toUpperCase());
            return (ret);
        }

        public void invoke(Connection conn, Procedure proc, long subscriberSize) throws SQLException {
            this.generator.invoke(conn, proc, subscriberSize);
        }

    } // TRANSCTION ENUM

    private final long subscriberSize;

    public TAOltpWorker(TABenchmark benchmarkModule, int id) {
        super(benchmarkModule, id);
        this.subscriberSize = Math.round(TAConstants.DEFAULT_NUM_SUBSCRIBERS * benchmarkModule.getWorkloadConfiguration().getScaleFactor());
    }

    @Override
    protected TransactionStatus executeWork(TransactionType txnType) throws UserAbortException, SQLException {
        Transaction t = Transaction.get(txnType.getName());
        assert(t != null) : "Unexpected " + txnType;

        // Get the Procedure handle
        Procedure proc = this.getProcedure(txnType);
        assert(proc != null) : String.format("Failed to get Procedure handle for %s.%s",
                this.getBenchmarkModule().getBenchmarkName(), txnType);
        if (LOG.isDebugEnabled()) LOG.debug("Executing " + proc);

        t.invoke(this.conn, proc, subscriberSize);
        conn.commit();
        return (TransactionStatus.SUCCESS);
    }

}

