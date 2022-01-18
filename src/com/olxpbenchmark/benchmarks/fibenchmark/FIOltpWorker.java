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

import java.sql.SQLException;
import java.util.Arrays;

import com.olxpbenchmark.benchmarks.fibenchmark.FIBenchmark;
import com.olxpbenchmark.benchmarks.fibenchmark.FIConstants;
import org.apache.log4j.Logger;

import com.olxpbenchmark.api.Procedure.UserAbortException;
import com.olxpbenchmark.api.Procedure;
import com.olxpbenchmark.api.TransactionType;
import com.olxpbenchmark.api.Worker;
import com.olxpbenchmark.benchmarks.fibenchmark.procedures.oltp.Amalgamate;
import com.olxpbenchmark.benchmarks.fibenchmark.procedures.oltp.Balance;
import com.olxpbenchmark.benchmarks.fibenchmark.procedures.oltp.DepositChecking;
import com.olxpbenchmark.benchmarks.fibenchmark.procedures.oltp.SendPayment;
import com.olxpbenchmark.benchmarks.fibenchmark.procedures.oltp.TransactSavings;
import com.olxpbenchmark.benchmarks.fibenchmark.procedures.oltp.WriteCheck;
import com.olxpbenchmark.benchmarks.fibenchmark.procedures.olxp.X1;
import com.olxpbenchmark.benchmarks.fibenchmark.procedures.olxp.X2;
import com.olxpbenchmark.benchmarks.fibenchmark.procedures.olxp.X3;
import com.olxpbenchmark.benchmarks.fibenchmark.procedures.olxp.X4;
import com.olxpbenchmark.benchmarks.fibenchmark.procedures.olxp.X5;
import com.olxpbenchmark.benchmarks.fibenchmark.procedures.olxp.X6;

import com.olxpbenchmark.types.TransactionStatus;
import com.olxpbenchmark.util.RandomDistribution.*;

/**
 * SmallBank Benchmark Work Driver
 * Fuck yo couch.
 * @author pavlo
 *
 */
public class FIOltpWorker extends Worker<FIBenchmark> {
    private static final Logger LOG = Logger.getLogger(FIOltpWorker.class);

    private final Amalgamate procAmalgamate;
    private final X1 procX1;
    private final Balance procBalance;
    private final X2 procX2;
    private final DepositChecking procDepositChecking;
    private final X3 procX3;
    private final SendPayment procSendPayment;
    private final X4 procX4;
    private final TransactSavings procTransactSavings;
    private final X5 procX5;
    private final WriteCheck procWriteCheck;
    private final X6 procX6;



    private final DiscreteRNG rng;
    private final long numAccounts;
    private final int custNameLength;
    private final String custNameFormat;
    private final long custIdsBuffer[] = { -1l, -1l };

    public FIOltpWorker(FIBenchmark benchmarkModule, int id) {
        super(benchmarkModule, id);

        // This is a minor speed-up to avoid having to invoke the hashmap look-up
        // everytime we want to execute a txn. This is important to do on
        // a client machine with not a lot of cores
        this.procAmalgamate = this.getProcedure(Amalgamate.class);
        this.procX1 = this.getProcedure(X1.class);
        this.procBalance = this.getProcedure(Balance.class);
        this.procX2 = this.getProcedure(X2.class);
        this.procDepositChecking = this.getProcedure(DepositChecking.class);
        this.procX3 = this.getProcedure(X3.class);
        this.procSendPayment = this.getProcedure(SendPayment.class);
        this.procX4 = this.getProcedure(X4.class);
        this.procTransactSavings = this.getProcedure(TransactSavings.class);
        this.procX5 = this.getProcedure(X5.class);
        this.procWriteCheck = this.getProcedure(WriteCheck.class);
        this.procX6 = this.getProcedure(X6.class);


        this.numAccounts = benchmarkModule.numAccounts;
        this.custNameLength = FIBenchmark.getCustomerNameLength(benchmarkModule.getTableCatalog(FIConstants.TABLENAME_ACCOUNTS));
        this.custNameFormat = "%0"+this.custNameLength+"d";
        this.rng = new Flat(rng(), 0, this.numAccounts);
    }

    protected void generateCustIds(boolean needsTwoAccts) {
        for (int i = 0; i < this.custIdsBuffer.length; i++) {
            this.custIdsBuffer[i] = this.rng.nextLong();

            // They can never be the same!
            if (i > 0 && this.custIdsBuffer[i-1] == this.custIdsBuffer[i]) {
                i--;
                continue;
            }

            // If we only need one acctId, break out here.
            if (i == 0 && needsTwoAccts == false) break;
            // If we need two acctIds, then we need to go generate the second one
            if (i == 0) continue;

        } // FOR
        if (LOG.isDebugEnabled())
            LOG.debug(String.format("Accounts: %s", Arrays.toString(this.custIdsBuffer)));
    }


    @Override
    protected TransactionStatus executeWork(TransactionType txnType) throws UserAbortException, SQLException {
        Class<? extends Procedure> procClass = txnType.getProcedureClass();

        // Amalgamate
        if (procClass.equals(Amalgamate.class)) {
            this.generateCustIds(true);
            this.procAmalgamate.run(conn, this.custIdsBuffer[0], this.custIdsBuffer[1]);
        } else  if (procClass.equals(X1.class)) {
            this.generateCustIds(true);
            this.procX1.run(conn, this.custIdsBuffer[0], this.custIdsBuffer[1]);

            // Balance
        } else if (procClass.equals(Balance.class)) {
            this.generateCustIds(false);
            String custName = String.format(this.custNameFormat, this.custIdsBuffer[0]);
            this.procBalance.run(conn, custName);
            //X2
        } else if (procClass.equals(X2.class)) {
                this.generateCustIds(false);
                String custName = String.format(this.custNameFormat, this.custIdsBuffer[0]);
                this.procX2.run(conn, custName);
            // DepositChecking
        } else if (procClass.equals(DepositChecking.class)) {
            this.generateCustIds(false);
            String custName = String.format(this.custNameFormat, this.custIdsBuffer[0]);
            this.procDepositChecking.run(conn, custName, FIConstants.PARAM_DEPOSIT_CHECKING_AMOUNT);
            //X3
        } else if (procClass.equals(X3.class)) {
            this.generateCustIds(false);
            String custName = String.format(this.custNameFormat, this.custIdsBuffer[0]);
            this.procX3.run(conn, custName, FIConstants.PARAM_DEPOSIT_CHECKING_AMOUNT);
            // SendPayment
        } else if (procClass.equals(SendPayment.class)) {
            this.generateCustIds(true);
            this.procSendPayment.run(conn, this.custIdsBuffer[0], this.custIdsBuffer[0], FIConstants.PARAM_SEND_PAYMENT_AMOUNT);
            // X4
        } else if (procClass.equals(X4.class)) {
            this.generateCustIds(true);
            this.procX4.run(conn, this.custIdsBuffer[0], this.custIdsBuffer[0], FIConstants.PARAM_SEND_PAYMENT_AMOUNT);

            // TransactSavings
        } else if (procClass.equals(TransactSavings.class)) {
            this.generateCustIds(false);
            String custName = String.format(this.custNameFormat, this.custIdsBuffer[0]);
            this.procTransactSavings.run(conn, custName, FIConstants.PARAM_TRANSACT_SAVINGS_AMOUNT);
            //X5
        } else if (procClass.equals(X5.class)) {
            this.generateCustIds(false);
            String custName = String.format(this.custNameFormat, this.custIdsBuffer[0]);
            this.procX5.run(conn, custName, FIConstants.PARAM_TRANSACT_SAVINGS_AMOUNT);
            // WriteCheck
        } else if (procClass.equals(WriteCheck.class)) {
            this.generateCustIds(false);
            String custName = String.format(this.custNameFormat, this.custIdsBuffer[0]);
            this.procWriteCheck.run(conn, custName, FIConstants.PARAM_WRITE_CHECK_AMOUNT);
            // X6
        } else if (procClass.equals(X6.class)) {
            this.generateCustIds(false);
            String custName = String.format(this.custNameFormat, this.custIdsBuffer[0]);
            this.procX6.run(conn, custName, FIConstants.PARAM_WRITE_CHECK_AMOUNT);
        }
        conn.commit();

        return TransactionStatus.SUCCESS;
    }

}


