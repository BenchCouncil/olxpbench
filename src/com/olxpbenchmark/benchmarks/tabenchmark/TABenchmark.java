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

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.olxpbenchmark.WorkloadConfiguration;
import com.olxpbenchmark.api.BenchmarkModule;
import com.olxpbenchmark.api.Loader;
import com.olxpbenchmark.api.Worker;
import com.olxpbenchmark.benchmarks.tabenchmark.procedures.olap.Q1;
import com.olxpbenchmark.benchmarks.tabenchmark.procedures.oltp.DeleteCallForwarding;
import com.olxpbenchmark.benchmarks.tabenchmark.procedures.olxp.X1;

public class TABenchmark extends BenchmarkModule {

    public TABenchmark(WorkloadConfiguration workConf) {
        super("tabenchmark", workConf, true);
    }

    @Override
    protected Package getProcedurePackageImpl(String txnName) {
        if(txnName.startsWith("Q"))
            return (Q1.class.getPackage());
        else if(txnName.startsWith("X"))
            return (X1.class.getPackage());
        else
            return DeleteCallForwarding.class.getPackage();
    }

    @Override
    protected List<Worker<? extends BenchmarkModule>> makeWorkersImpl(boolean verbose) throws IOException {
        List<Worker<? extends BenchmarkModule>> workers = new ArrayList<Worker<? extends BenchmarkModule>>();
        for (int i = 0; i < workConf.getOltpTerminals(); ++i) {
            workers.add(new TAOltpWorker(this, i));
        } // FOR
        return (workers);
    }

    //everyadd
    protected List<Worker<? extends BenchmarkModule>> makeWorkersImpl(boolean verbose, String workloadType)throws IOException {
        List<Worker<? extends BenchmarkModule>> workers = new ArrayList<Worker<? extends BenchmarkModule>>();
        if(workloadType.equals("oltp")) {
            for (int i = 0; i < workConf.getOltpTerminals(); ++i) {
                workers.add(new TAOltpWorker(this, i));
            }
        } else if(workloadType.equals("olxp")) {
            for (int i = 0; i < workConf.getOlxpTerminals(); ++i) {
                workers.add(new TAOltpWorker(this, i));
            }
        } else if(workloadType.equals("olap")) {
            for (int i = 0; i < workConf.getOlapTerminals(); i++) {
                workers.add(new TAOlapWorker(this, i));
            }}
        return workers;
    }

    @Override
    protected Loader<TABenchmark> makeLoaderImpl() throws SQLException {
        return (new TALoader(this));
    }
}

