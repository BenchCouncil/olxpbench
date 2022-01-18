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

package com.olxpbenchmark.benchmarks.fibenchmark;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.olxpbenchmark.api.Loader;
import com.olxpbenchmark.api.Loader.LoaderThread;
import com.olxpbenchmark.catalog.Table;
import com.olxpbenchmark.util.SQLUtil;
import com.olxpbenchmark.util.RandomDistribution.*;

/**
 * SmallBankBenchmark Loader
 * @author pavlo
 */
public class FILoader extends Loader<FIBenchmark> {
    private static final Logger LOG = Logger.getLogger(FILoader.class);

    private final Table catalogAccts;
    private final Table catalogSavings;
    private final Table catalogChecking;

    private final String sqlAccts;
    private final String sqlSavings;
    private final String sqlChecking;

    private final long numAccounts;
    private final int custNameLength;

    public FILoader(FIBenchmark benchmark) {
        super(benchmark);

        this.catalogAccts = this.benchmark.getTableCatalog(FIConstants.TABLENAME_ACCOUNTS);
        assert(this.catalogAccts != null);
        this.catalogSavings = this.benchmark.getTableCatalog(FIConstants.TABLENAME_SAVINGS);
        assert(this.catalogSavings != null);
        this.catalogChecking = this.benchmark.getTableCatalog(FIConstants.TABLENAME_CHECKING);
        assert(this.catalogChecking != null);

        this.sqlAccts = SQLUtil.getInsertSQL(this.catalogAccts, this.getDatabaseType());
        this.sqlSavings = SQLUtil.getInsertSQL(this.catalogSavings, this.getDatabaseType());
        this.sqlChecking = SQLUtil.getInsertSQL(this.catalogChecking, this.getDatabaseType());

        this.numAccounts = benchmark.numAccounts;
        this.custNameLength = FIBenchmark.getCustomerNameLength(this.catalogAccts);
    }

    @Override
    public List<LoaderThread> createLoaderThreads() throws SQLException {
        List<LoaderThread> threads = new ArrayList<LoaderThread>();
        int batchSize = 100000;
        long start = 0;
        while (start < this.numAccounts) {
            long stop = Math.min(start + batchSize, this.numAccounts);
            threads.add(new Generator(start, stop));
            start = stop;
        }
        return (threads);
    }

    /**
     * Thread that can generate a range of accounts
     */
    private class Generator extends LoaderThread {
        private final long start;
        private final long stop;
        private final DiscreteRNG randBalance;

        PreparedStatement stmtAccts;
        PreparedStatement stmtSavings;
        PreparedStatement stmtChecking;

        public Generator(long start, long stop) throws SQLException {
            super();
            this.start = start;
            this.stop = stop;
            this.randBalance = new Gaussian(FILoader.this.benchmark.rng(),
                    FIConstants.MIN_BALANCE,
                    FIConstants.MAX_BALANCE);
        }

        @Override
        public void load(Connection conn) throws SQLException {
            try {
                this.stmtAccts = conn.prepareStatement(FILoader.this.sqlAccts);
                this.stmtSavings = conn.prepareStatement(FILoader.this.sqlSavings);
                this.stmtChecking = conn.prepareStatement(FILoader.this.sqlChecking);

                final String acctNameFormat = "%0"+custNameLength+"d";
                int batchSize = 0;
                for (long acctId = this.start; acctId < this.stop; acctId++) {
                    // ACCOUNT
                    String acctName = String.format(acctNameFormat, acctId);
                    stmtAccts.setLong(1, acctId);
                    stmtAccts.setString(2, acctName);
                    stmtAccts.addBatch();

                    // CHECKINGS
                    stmtChecking.setLong(1, acctId);
                    stmtChecking.setInt(2, this.randBalance.nextInt());
                    stmtChecking.addBatch();

                    // SAVINGS
                    stmtSavings.setLong(1, acctId);
                    stmtSavings.setInt(2, this.randBalance.nextInt());
                    stmtSavings.addBatch();

                    if (++batchSize >= FIConstants.BATCH_SIZE) {
                        this.loadTables(conn);
                        batchSize = 0;
                    }

                } // FOR
                if (batchSize > 0) {
                    this.loadTables(conn);
                }
            } catch (SQLException ex) {
                LOG.error("Failed to load data", ex);
                throw new RuntimeException(ex);
            }
        }

        private void loadTables(Connection conn) throws SQLException {
            this.stmtAccts.executeBatch();
            this.stmtSavings.executeBatch();
            this.stmtChecking.executeBatch();
            conn.commit();

        }
    };

}
