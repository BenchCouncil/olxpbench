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

package com.olxpbenchmark.benchmarks.subenchmark;

import com.olxpbenchmark.WorkloadConfiguration;
import com.olxpbenchmark.api.BenchmarkModule;
import com.olxpbenchmark.api.Loader;
import com.olxpbenchmark.api.Worker;
import com.olxpbenchmark.benchmarks.subenchmark.procedures.olxp.X1;
import com.olxpbenchmark.benchmarks.subenchmark.procedures.olap.Q1;
import com.olxpbenchmark.benchmarks.subenchmark.procedures.oltp.NewOrder;
import com.olxpbenchmark.benchmarks.subenchmark.SUConfig;
import com.olxpbenchmark.benchmarks.subenchmark.SUOlapWroker;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class SUBenchmark extends BenchmarkModule {
    private static final Logger LOG = Logger.getLogger(SUBenchmark.class);

    public SUBenchmark(WorkloadConfiguration workConf) {
        super("subenchmark", workConf, true);
    }

    @Override
    protected Package getProcedurePackageImpl(String txnName) {
        if(txnName.startsWith("Q"))
            return (Q1.class.getPackage());
        else if(txnName.startsWith("X"))
            return (X1.class.getPackage());
        else
            return (NewOrder.class.getPackage());
    }

    @Override
    protected List<Worker<? extends BenchmarkModule>> makeWorkersImpl(boolean verbose) throws IOException {
        ArrayList<Worker<? extends BenchmarkModule>> workers = new ArrayList<Worker<? extends BenchmarkModule>>();


        return workers;
    }

    //everyadd
    protected List<Worker<? extends BenchmarkModule>> makeWorkersImpl(boolean verbose, String workloadType)throws IOException {
        List<Worker<? extends BenchmarkModule>> workers = new ArrayList<Worker<? extends BenchmarkModule>>();
        if(workloadType.equals("oltp")) {
            try{
                List<SUOltpWroker> oltpTerminals = createOltpTerminals();
                workers.addAll(oltpTerminals);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else if(workloadType.equals("olxp")) {
            try{
                List<SUOltpWroker> olxpTerminals = createOlxpTerminals();
                workers.addAll(olxpTerminals);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else if (workloadType.equals("olap")) {
            try{
                int numOlapTerminals = workConf.getOlapTerminals();
                LOG.info(String.format("Creating %d workers for OLAP", numOlapTerminals));
                for (int i = 0; i < numOlapTerminals; i++) {
                    workers.add(new SUOlapWroker(this, i));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return workers;
    }

    protected ArrayList<SUOltpWroker> createOlxpTerminals() throws SQLException {

        SUOltpWroker[] olxpTerminals = new SUOltpWroker[workConf.getOlxpTerminals()];

        int numWarehouses = (int) workConf.getScaleFactor();//tpccConf.getNumWarehouses();
        if (numWarehouses <= 0) {
            numWarehouses = 1;
        }
        int numOlxpTerminals = workConf.getOlxpTerminals();
        assert (numOlxpTerminals >= numWarehouses) :
                String.format("Insufficient number of terminals '%d' [numWarehouses=%d]",
                        numOlxpTerminals, numWarehouses);

        // TODO: This is currently broken: fix it!
        int warehouseOffset = Integer.getInteger("warehouseOffset", 1);
        assert warehouseOffset == 1;

        // We distribute terminals evenly across the warehouses
        // Eg. if there are 10 terminals across 7 warehouses, they
        // are distributed as
        // 1, 1, 2, 1, 2, 1, 2
        final double terminalsPerWarehouse = (double) numOlxpTerminals
                / numWarehouses;
        int workerId = 0;
        assert terminalsPerWarehouse >= 1;
        for (int w = 0; w < numWarehouses; w++) {
            // Compute the number of terminals in *this* warehouse
            int lowerTerminalId = (int) (w * terminalsPerWarehouse);
            int upperTerminalId = (int) ((w + 1) * terminalsPerWarehouse);
            // protect against double rounding errors
            int w_id = w + 1;
            if (w_id == numWarehouses)
                upperTerminalId = numOlxpTerminals;
            int numWarehouseTerminals = upperTerminalId - lowerTerminalId;

            if (LOG.isDebugEnabled())
                LOG.debug(String.format("w_id %d = %d olxpTerminals [lower=%d / upper%d]",
                        w_id, numWarehouseTerminals, lowerTerminalId, upperTerminalId));

            final double districtsPerTerminal = SUConfig.configDistPerWhse
                    / (double) numWarehouseTerminals;
            assert districtsPerTerminal >= 1 :
                    String.format("Too many olxpTerminals [districtsPerTerminal=%.2f, numWarehouseTerminals=%d]",
                            districtsPerTerminal, numWarehouseTerminals);
            for (int terminalId = 0; terminalId < numWarehouseTerminals; terminalId++) {
                int lowerDistrictId = (int) (terminalId * districtsPerTerminal);
                int upperDistrictId = (int) ((terminalId + 1) * districtsPerTerminal);
                if (terminalId + 1 == numWarehouseTerminals) {
                    upperDistrictId = SUConfig.configDistPerWhse;
                }
                lowerDistrictId += 1;

                SUOltpWroker olxpTerminal = new SUOltpWroker(this, workerId++,
                        w_id, lowerDistrictId, upperDistrictId,
                        numWarehouses);
                olxpTerminals[lowerTerminalId + terminalId] = olxpTerminal;
            }

        }
        assert olxpTerminals[olxpTerminals.length - 1] != null;

        ArrayList<SUOltpWroker> ret = new ArrayList<SUOltpWroker>();
        for (SUOltpWroker w : olxpTerminals)
            ret.add(w);
        return ret;
    }


    protected ArrayList<SUOltpWroker> createOltpTerminals() throws SQLException {

        SUOltpWroker[] oltpTerminals = new SUOltpWroker[workConf.getOltpTerminals()];

        int numWarehouses = (int) workConf.getScaleFactor();//tpccConf.getNumWarehouses();
        if (numWarehouses <= 0) {
            numWarehouses = 1;
        }
        int numOltpTerminals = workConf.getOltpTerminals();
        assert (numOltpTerminals >= numWarehouses) :
                String.format("Insufficient number of terminals '%d' [numWarehouses=%d]",
                        numOltpTerminals, numWarehouses);

        // TODO: This is currently broken: fix it!
        int warehouseOffset = Integer.getInteger("warehouseOffset", 1);
        assert warehouseOffset == 1;

        // We distribute terminals evenly across the warehouses
        // Eg. if there are 10 terminals across 7 warehouses, they
        // are distributed as
        // 1, 1, 2, 1, 2, 1, 2
        final double terminalsPerWarehouse = (double) numOltpTerminals
                / numWarehouses;
        int workerId = 0;
        assert terminalsPerWarehouse >= 1;
        for (int w = 0; w < numWarehouses; w++) {
            // Compute the number of terminals in *this* warehouse
            int lowerTerminalId = (int) (w * terminalsPerWarehouse);
            int upperTerminalId = (int) ((w + 1) * terminalsPerWarehouse);
            // protect against double rounding errors
            int w_id = w + 1;
            if (w_id == numWarehouses)
                upperTerminalId = numOltpTerminals;
            int numWarehouseTerminals = upperTerminalId - lowerTerminalId;

            if (LOG.isDebugEnabled())
                LOG.debug(String.format("w_id %d = %d oltpTerminals [lower=%d / upper%d]",
                        w_id, numWarehouseTerminals, lowerTerminalId, upperTerminalId));

            final double districtsPerTerminal = SUConfig.configDistPerWhse
                    / (double) numWarehouseTerminals;
            assert districtsPerTerminal >= 1 :
                    String.format("Too many oltpTerminals [districtsPerTerminal=%.2f, numWarehouseTerminals=%d]",
                            districtsPerTerminal, numWarehouseTerminals);
            for (int terminalId = 0; terminalId < numWarehouseTerminals; terminalId++) {
                int lowerDistrictId = (int) (terminalId * districtsPerTerminal);
                int upperDistrictId = (int) ((terminalId + 1) * districtsPerTerminal);
                if (terminalId + 1 == numWarehouseTerminals) {
                    upperDistrictId = SUConfig.configDistPerWhse;
                }
                lowerDistrictId += 1;

                SUOltpWroker oltpTerminal = new SUOltpWroker(this, workerId++,
                        w_id, lowerDistrictId, upperDistrictId,
                        numWarehouses);
                oltpTerminals[lowerTerminalId + terminalId] = oltpTerminal;
            }

        }
        assert oltpTerminals[oltpTerminals.length - 1] != null;

        ArrayList<SUOltpWroker> ret = new ArrayList<SUOltpWroker>();
        for (SUOltpWroker w : oltpTerminals)
            ret.add(w);
        return ret;
    }
    //everyadd

    @Override
    protected Loader<SUBenchmark> makeLoaderImpl() throws SQLException {
        return new SULoader(this);
    }

    /**
     * Hack to support postgres-specific timestamps
     * @param time
     * @return
     */
    public Timestamp getTimestamp(long time) {
        Timestamp timestamp;

        // 2020-03-03: I am no longer aware of any DBMS that needs a specialized data type for timestamps.
        timestamp = new java.sql.Timestamp(time);

        return (timestamp);
    }

    }
