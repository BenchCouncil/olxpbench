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

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.olxpbenchmark.WorkloadConfiguration;
import com.olxpbenchmark.api.BenchmarkModule;
import com.olxpbenchmark.api.Loader;
import com.olxpbenchmark.api.Worker;
import com.olxpbenchmark.benchmarks.fibenchmark.procedures.oltp.Amalgamate;
import com.olxpbenchmark.benchmarks.fibenchmark.procedures.olap.Q1;
import com.olxpbenchmark.benchmarks.fibenchmark.procedures.olxp.X1;
import com.olxpbenchmark.catalog.Column;
import com.olxpbenchmark.catalog.Table;
import com.olxpbenchmark.util.SQLUtil;

public class FIBenchmark extends BenchmarkModule {

    protected final long numAccounts;

    public FIBenchmark(WorkloadConfiguration workConf) {
        super("fibenchmark", workConf, true);
        this.numAccounts = (int)Math.round(FIConstants.NUM_ACCOUNTS * workConf.getScaleFactor());
    }

    @Override
    protected List<Worker<? extends BenchmarkModule>> makeWorkersImpl(boolean verbose) throws IOException {
        List<Worker<? extends BenchmarkModule>> workers = new ArrayList<Worker<? extends BenchmarkModule>>();
        for (int i = 0; i < workConf.getOltpTerminals(); ++i) {
            workers.add(new FIOltpWorker(this, i));
        }
        return workers;
    }

    //everyadd
    protected List<Worker<? extends BenchmarkModule>> makeWorkersImpl(boolean verbose, String workloadType)throws IOException {
        List<Worker<? extends BenchmarkModule>> workers = new ArrayList<Worker<? extends BenchmarkModule>>();
/*        for (int i = 0; i < workConf.getOltpTerminals(); ++i) {
            workers.add(new FIOltpWorker(this, i));
        }*/
        if(workloadType.equals("oltp")) {
            try{
                for (int i = 0; i < workConf.getOltpTerminals(); ++i) {
                    workers.add(new FIOltpWorker(this, i));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else if(workloadType.equals("olxp")) {
            try{
                for (int i = 0; i < workConf.getOlxpTerminals(); ++i) {
                    workers.add(new FIOltpWorker(this, i));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        else if (workloadType.equals("olap")) {
            try{
                for (int i = 0; i < workConf.getOlapTerminals(); i++) {
                    workers.add(new FIOlapWorker(this, i));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return workers;
    }

    @Override
    protected Loader<FIBenchmark> makeLoaderImpl() throws SQLException {
        return new FILoader(this);
    }

    //everyadd txn
    @Override
    protected Package getProcedurePackageImpl(String txnName) {
        if(txnName.startsWith("Q"))
            return (Q1.class.getPackage());
        else if(txnName.startsWith("X")) {
            return (X1.class.getPackage());
        }
        else
            return Amalgamate.class.getPackage();
    }


    /**
     * For the given table, return the length of the first VARCHAR attribute
     * @param acctsTbl
     * @return
     */
    public static int getCustomerNameLength(Table acctsTbl) {
        int acctNameLength = -1;
        for (Column col : acctsTbl.getColumns()) {
            if (SQLUtil.isStringType(col.getType())) {
                acctNameLength = col.getSize();
                break;
            }
        } // FOR
        assert(acctNameLength > 0);
        return (acctNameLength);
    }

}

