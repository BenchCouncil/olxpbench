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

import com.olxpbenchmark.api.Procedure.UserAbortException;
import com.olxpbenchmark.api.TransactionType;
import com.olxpbenchmark.api.Worker;
import com.olxpbenchmark.benchmarks.subenchmark.procedures.olap.GenericQuery;
import com.olxpbenchmark.types.TransactionStatus;
import com.olxpbenchmark.util.RandomGenerator;

import java.sql.SQLException;

public class SUOlapWroker extends Worker<SUBenchmark> {

    private final RandomGenerator rand;

    public SUOlapWroker(SUBenchmark benchmarkModule, int id) {
        super(benchmarkModule, id);
        this.rng().setSeed(15721);
        rand = new RandomGenerator(this.rng().nextInt());
    }

    @Override
    protected TransactionStatus executeWork(TransactionType nextTransaction) throws UserAbortException, SQLException {
        try {
            GenericQuery proc = (GenericQuery) this.getProcedure(nextTransaction.getProcedureClass());
            proc.setOwner(this);
            proc.run(conn, rand);
        } catch (ClassCastException e) {
            System.err.println("We have been invoked with an INVALID transactionType?!");
            throw new RuntimeException("Bad transaction type = "+ nextTransaction);
        }

        conn.commit();
        return (TransactionStatus.SUCCESS);

    }
}
