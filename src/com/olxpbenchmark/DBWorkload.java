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
package com.olxpbenchmark;

/*
 * Copyright 2015 by OLTPBenchmark Project

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

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.SubnodeConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.configuration.tree.xpath.XPathExpressionEngine;
import org.apache.log4j.Logger;

import com.olxpbenchmark.api.BenchmarkModule;
import com.olxpbenchmark.api.TransactionType;
import com.olxpbenchmark.api.TransactionTypes;
import com.olxpbenchmark.api.Worker;
import com.olxpbenchmark.types.DatabaseType;
import com.olxpbenchmark.util.ClassUtil;
import com.olxpbenchmark.util.FileUtil;
import com.olxpbenchmark.util.QueueLimitException;
import com.olxpbenchmark.util.ResultUploader;
import com.olxpbenchmark.util.StringBoxUtil;
import com.olxpbenchmark.util.StringUtil;
import com.olxpbenchmark.util.TimeUtil;
import com.olxpbenchmark.util.JSONUtil;
import com.olxpbenchmark.util.JSONSerializable;

public class DBWorkload {
    private static final Logger LOG = Logger.getLogger(DBWorkload.class);
    
    private static final String SINGLE_LINE = StringUtil.repeat("=", 70);
    
    private static final String RATE_DISABLED = "disabled";
    private static final String RATE_UNLIMITED = "unlimited";
    
    /**
     * @param args
     * @throws Exception 
     */
    public static void main(String[] args) throws Exception {
        // Initialize log4j
        String log4jPath = System.getProperty("log4j.configuration");
        if (log4jPath != null) {
            org.apache.log4j.PropertyConfigurator.configure(log4jPath);
        } else {
            throw new RuntimeException("Missing log4j.properties file");
        }
        
        if (ClassUtil.isAssertsEnabled()) {
            LOG.warn("\n" + getAssertWarning());
        }
        
        // create the command line parser
        CommandLineParser parser = new PosixParser();
        XMLConfiguration pluginConfig=null;
        try {
            pluginConfig = new XMLConfiguration("config/plugin.xml");
        } catch (ConfigurationException e1) {
            LOG.info("Plugin configuration file config/plugin.xml is missing");
            e1.printStackTrace();
        }
        pluginConfig.setExpressionEngine(new XPathExpressionEngine());
        Options options = new Options();
        options.addOption(
                "b",
                "bench",
                true,
                "[required] Benchmark class. Currently supported: "+ pluginConfig.getList("/plugin//@name"));
        options.addOption(
                "c", 
                "config", 
                true,
                "[required] Workload configuration file");
        options.addOption(
                null,
                "create",
                true,
                "Initialize the database for this benchmark");
        options.addOption(
                null,
                "clear",
                true,
                "Clear all records in the database for this benchmark");
        options.addOption(
                null,
                "load",
                true,
                "Load data using the benchmark's data loader");
        options.addOption(
                null,
                "execute",
                true,
                "Execute the benchmark workload");
        options.addOption(
                null,
                "runscript",
                true,
                "Run an SQL script");
        options.addOption(
                null,
                "upload",
                true,
                "Upload the result");
        options.addOption(
                null,
                "uploadHash",
                true,
                "git hash to be associated with the upload");
        options.addOption(
                "wt",
                "workloadType",
                true,
                " workloadType. Currently supported: oltp, olap. ");

        options.addOption("v", "verbose", false, "Display Messages");
        options.addOption("h", "help", false, "Print this help");
        options.addOption("s", "sample", true, "Sampling window");
        options.addOption("im", "interval-monitor", true, "Throughput Monitoring Interval in milliseconds");
        options.addOption("ss", false, "Verbose Sampling per Transaction");
        options.addOption("o", "output", true, "Output file (default System.out)");
        options.addOption("d", "directory", true, "Base directory for the result files, default is current directory");
        options.addOption("t", "timestamp", false, "Each result file is prepended with a timestamp for the beginning of the experiment");
        options.addOption("ts", "tracescript", true, "Script of transactions to execute");
        options.addOption(null, "histograms", false, "Print txn histograms");
        options.addOption("jh", "json-histograms", true, "Export histograms to JSON file");
        options.addOption(null, "dialects-export", true, "Export benchmark SQL to a dialects file");
        options.addOption(null, "output-raw", true, "Output raw data");
        options.addOption(null, "output-samples", true, "Output sample data");


        // parse the command line arguments
        CommandLine argsLine = parser.parse(options, args);
        if (argsLine.hasOption("h")) {
            printUsage(options);
            return;
        } else if (argsLine.hasOption("c") == false) {
            LOG.error("Missing Configuration file");
            printUsage(options);
            return;
        } else if (argsLine.hasOption("b") == false) {
            LOG.fatal("Missing Benchmark Class to load");
            printUsage(options);
            return;
        }
        
        
        
        // Seconds
        int intervalMonitor = 0;
        if (argsLine.hasOption("im")) {
            intervalMonitor = Integer.parseInt(argsLine.getOptionValue("im"));
        }
        
        // -------------------------------------------------------------------
        // GET PLUGIN LIST
        // -------------------------------------------------------------------
        
        String targetBenchmarks = argsLine.getOptionValue("b");
        
        String[] targetList = targetBenchmarks.split(",");
        List<BenchmarkModule> benchList = new ArrayList<BenchmarkModule>();

        //everyadd
        String wTypes = argsLine.getOptionValue("wt");;
        //System.out.println("wTypes  is : " + wTypes);
        String[] workloads = wTypes.split(",");
        //everyadd
        
        // Use this list for filtering of the output
        List<TransactionType> activeTXTypes = new ArrayList<TransactionType>();
        
        String configFile = argsLine.getOptionValue("c");
        XMLConfiguration xmlConfig = new XMLConfiguration(configFile);
        xmlConfig.setExpressionEngine(new XPathExpressionEngine());

        // Load the configuration for each benchmark
        int lastTxnId = 0;
        for(String w : workloads) {
            //for (String plugin : targetList) {
                String plugin = targetList[0];
                String pluginTest = "[@bench='" + plugin + "']";
                //System.out.println("pluginTest= " + pluginTest);

                // ----------------------------------------------------------------
                // BEGIN LOADING WORKLOAD CONFIGURATION
                // ----------------------------------------------------------------

                WorkloadConfiguration wrkld = new WorkloadConfiguration();
                wrkld.setBenchmarkName(plugin);
                wrkld.setXmlConfig(xmlConfig);
                boolean scriptRun = false;
                if (argsLine.hasOption("t")) {
                    scriptRun = true;
                    String traceFile = argsLine.getOptionValue("t");
                    wrkld.setTraceReader(new TraceReader(traceFile));
                    if (LOG.isDebugEnabled()) LOG.debug(wrkld.getTraceReader().toString());
                }

                // Pull in database configuration
                wrkld.setDBType(DatabaseType.get(xmlConfig.getString("dbtype")));
                wrkld.setDBDriver(xmlConfig.getString("driver"));
                wrkld.setDBConnection(xmlConfig.getString("DBUrl"));
                wrkld.setDBName(xmlConfig.getString("DBName"));
                wrkld.setDBUsername(xmlConfig.getString("username"));
                wrkld.setDBPassword(xmlConfig.getString("password"));

                int oltpTerminals = xmlConfig.getInt("oltpTerminals[not(@bench)]", 0);
                oltpTerminals = xmlConfig.getInt("oltpTerminals" + pluginTest, oltpTerminals);

                wrkld.setOltpTerminals(oltpTerminals);

                //everyadd | Load the sum of olap terminals.
                int olapTerminals = xmlConfig.getInt("olapTerminals[not(@bench)]", 0);
                olapTerminals = xmlConfig.getInt("olapTerminals" + pluginTest, olapTerminals);
                wrkld.setOlapTerminals(olapTerminals);

                int olxpTerminals = xmlConfig.getInt("olxpTerminals[not(@bench)]", 0);
                olxpTerminals = xmlConfig.getInt("olxpTerminals" + pluginTest, olxpTerminals);
                wrkld.setOlxpTerminals(olxpTerminals);
                //everyadd

                if (xmlConfig.containsKey("loaderThreads")) {
                    int loaderThreads = xmlConfig.getInt("loaderThreads");
                    wrkld.setLoaderThreads(loaderThreads);
                }

                String isolationMode = xmlConfig.getString("isolation[not(@bench)]", "TRANSACTION_SERIALIZABLE");
                wrkld.setIsolationMode(xmlConfig.getString("isolation" + pluginTest, isolationMode));
                wrkld.setScaleFactor(xmlConfig.getDouble("scalefactor", 1.0));
                wrkld.setRecordAbortMessages(xmlConfig.getBoolean("recordabortmessages", false));
                wrkld.setDataDir(xmlConfig.getString("datadir", "."));

                double selectivity = -1;
                try {
                    selectivity = xmlConfig.getDouble("selectivity");
                    wrkld.setSelectivity(selectivity);
                } catch (NoSuchElementException nse) {
                    // Nothing to do here !
                }

                // ----------------------------------------------------------------
                // CREATE BENCHMARK MODULE
                // ----------------------------------------------------------------

                String classname = pluginConfig.getString("/plugin[@name='" + plugin + "']");

                if (classname == null)
                    throw new ParseException("Plugin " + plugin + " is undefined in config/plugin.xml");
                BenchmarkModule bench = ClassUtil.newInstance(classname, new Object[]{wrkld}, new Class<?>[]{WorkloadConfiguration.class});
                Map<String, Object> initDebug = new ListOrderedMap<String, Object>();
                initDebug.put("Benchmark", String.format("%s {%s}", plugin.toUpperCase(), classname));
                initDebug.put("Configuration", configFile);
                initDebug.put("Type", wrkld.getDBType());
                initDebug.put("Driver", wrkld.getDBDriver());
                initDebug.put("URL", wrkld.getDBConnection());
                initDebug.put("Isolation", wrkld.getIsolationString());
                initDebug.put("Scale Factor", wrkld.getScaleFactor());

                if (selectivity != -1)
                    initDebug.put("Selectivity", selectivity);

                LOG.info(SINGLE_LINE + "\n\n" + StringUtil.formatMaps(initDebug));
                LOG.info(SINGLE_LINE);

                // ----------------------------------------------------------------
                // LOAD TRANSACTION DESCRIPTIONS
                // ----------------------------------------------------------------

                //everyadd
                String workloadTest = "[@workloadType='" + w + "']";
                int numTxnTypes = xmlConfig.configurationsAt("transactiontypes" + workloadTest + "/transactiontype").size();
                //System.out.println("numTxnTypes = " + numTxnTypes);
                assert (numTxnTypes > 0);
                if (w.equals("oltp")) {
                    //System.out.println("workloadTest = " + workloadTest); workloadTest = [@workloadType='oltp']
                    wrkld.setNumTxnTypes(numTxnTypes);
                    List<TransactionType> oltpTTypes = new ArrayList<TransactionType>();
                    oltpTTypes.add(TransactionType.INVALID);
                    int oltpTxnIDOffset = lastTxnId;
                    for (int i = 1; i <= wrkld.getNumTxnTypes(); i++) {
                        String key = "transactiontypes" + workloadTest + "/transactiontype[" + i + "]";
                        String txnName = xmlConfig.getString(key + "/name");
                        int txnId = i;
                        TransactionType tmpType = bench.initTransactionType(txnName, txnId + oltpTxnIDOffset);
                        // Keep a reference for filtering
                        activeTXTypes.add(tmpType);

                        // Add a ref for the active TTypes in this benchmark
                        oltpTTypes.add(tmpType);
                        lastTxnId = i;
                    }

                    // Wrap the list of transactions and save them
                    TransactionTypes tt = new TransactionTypes(oltpTTypes);
                    wrkld.setTransTypes(tt);
                    LOG.debug("Using the following transaction types: " + tt);

                    HashMap<String, List<String>> groupings = new HashMap<String, List<String>>();

                    // even weight to all transactions in the benchmark.
                    List<String> weightAll = new ArrayList<String>();
                    for (int i = 0; i < numTxnTypes; ++i)
                        weightAll.add("1");
                    groupings.put("all", weightAll);
                    benchList.add(bench);

                    // ----------------------------------------------------------------
                    // WORKLOAD CONFIGURATION
                    // ----------------------------------------------------------------

                    SubnodeConfiguration work = xmlConfig.configurationAt("works/work[1]");
                    List<String> oltp_weight_strings = null;

                    if (work.containsKey("weights[@workloadType]")) {
                        String weightKey = work.getString("weights" + workloadTest).toLowerCase();
                        oltp_weight_strings = getWeights(w, work);
                        //System.out.println("weight_strings[0] = " + weight_strings.get(0)); weight_strings[0] = 45
                    } else {
                        LOG.fatal(String.format("Invalid string for weight. workloadType string must be 'true'"));
                        System.exit(-1);
                    }

                    int rate = 1;
                    boolean rateLimited = true;
                    boolean disabled = false;
                    boolean serial = false;
                    boolean timed = false;

                    // can be "disabled", "unlimited" or a number
                    String rate_string;
                    rate_string = work.getString("rate[not(@workloadType)]", "");
                    rate_string = work.getString("rate" + workloadTest, rate_string);
                    if (rate_string.equals(RATE_DISABLED)) {
                        disabled = true;
                    } else if (rate_string.equals(RATE_UNLIMITED)) {
                        rateLimited = false;
                    } else if (rate_string.isEmpty()) {
                        LOG.fatal(String.format("Please specify the rate for phase %d and workload %s", 1, w));
                        System.exit(-1);
                    } else {
                        try {
                            rate = Integer.parseInt(rate_string);
                            if (rate < 1) {
                                LOG.fatal("Rate limit must be at least 1. Use unlimited or disabled values instead.");
                                System.exit(-1);
                            }
                        } catch (NumberFormatException e) {
                            LOG.fatal(String.format("Rate string must be '%s', '%s' or a number", RATE_DISABLED, RATE_UNLIMITED));
                            System.exit(-1);
                        }
                    }
                    Phase.Arrival arrival = Phase.Arrival.REGULAR;
                    String arrive = work.getString("@arrival", "regular");
                    if (arrive.toUpperCase().equals("POISSON"))
                        arrival = Phase.Arrival.POISSON;

                    // If serial is enabled then run all queries exactly once in serial (rather than
                    // random) order
                    String serial_string;
                    serial_string = work.getString("serial[not(@bench)]", "false");
                    serial_string = work.getString("serial" + pluginTest, serial_string);
                    if (serial_string.equals("true")) {
                        serial = true;
                    } else if (serial_string.equals("false")) {
                        serial = false;
                    } else {
                        LOG.fatal(String.format("Invalid string for serial: '%s'. Serial string must be 'true' or 'false'",
                                serial_string));
                        System.exit(-1);
                    }

                    // We're not actually serial if we're running a script, so make
                    // sure to suppress the serial flag in this case.
                    serial = serial && (wrkld.getTraceReader() == null);

                    int activeTerminals;
                    activeTerminals = work.getInt("active_terminals[not(@bench)]", oltpTerminals);
                    activeTerminals = work.getInt("active_terminals" + pluginTest, activeTerminals);
                    if (activeTerminals > (oltpTerminals + olapTerminals)) {
                        LOG.error(String.format("Configuration error in work %d: "
                                + "Number of active terminals is bigger than the total number of (oltpTerminals+olapTerminals)", 1));
                        System.exit(-1);
                    }

                    int time = work.getInt("/time", 0);
                    int warmup = work.getInt("/warmup", 0);
                    timed = (time > 0);

                    // System.out.println("!timed: " + !timed + "  serial : " + serial);

                    if (scriptRun) {
                        LOG.info("Running a script; ignoring timer, serial, and weight settings.");
                    } else if (!timed) {
                        if (serial) {
                            if (activeTerminals > 1) {
                                // For serial executions, we usually want only one terminal, but not always!
                                // (e.g. the CHBenCHmark)
                                LOG.warn("\n" + StringBoxUtil.heavyBox(String.format(
                                        "WARNING: Serial execution is enabled but the number of active terminals[=%d] > 1.\nIs this intentional??",
                                        activeTerminals)));
                            }
                            LOG.info("Timer disabled for serial run; will execute"
                                    + " all queries exactly once.");
                        } else {
                            LOG.fatal("Must provide positive time bound for"
                                    + " non-serial executions. Either provide"
                                    + " a valid time or enable serial mode.");
                            System.exit(-1);
                        }
                    } else if (serial)
                        LOG.info("Timer enabled for serial run; will run queries"
                                + " serially in a loop until the timer expires.");
                    if (warmup < 0) {
                        LOG.fatal("Must provide nonnegative time bound for"
                                + " warmup.");
                        System.exit(-1);
                    }

                    wrkld.addWork(time,
                            warmup,
                            rate,
                            oltp_weight_strings,
                            rateLimited,
                            disabled,
                            serial,
                            timed,
                            activeTerminals,
                            arrival);

                    // CHECKING INPUT PHASES
/*                    int j = 0;
                    for (Phase p : wrkld.getAllPhases()) {
                        j++;
                        if (p.getWeightCount() != wrkld.getNumTxnTypes()) {
                            LOG.fatal(String.format("Configuration files is inconsistent, phase %d contains %d weights but you defined %d transaction types",
                                    j, p.getWeightCount(), wrkld.getNumTxnTypes()));
                            if (p.isSerial()) {
                                LOG.fatal("However, note that since this a serial phase, the weights are irrelevant (but still must be included---sorry).");
                            }
                            System.exit(-1);
                        }
                        //System.out.println("weight count is : " + p.getWeightCount() + " num txn is " + wrkld.getNumTxnTypes());

                    } // FOR*/

                } else if (w.equals("olap")) {
                    //System.out.println("workloadTest = " + workloadTest); workloadTest = [@workloadType='oltp']
                    wrkld.setNumTxnTypes(numTxnTypes);
                    List<TransactionType> olapTTypes = new ArrayList<TransactionType>();
                    olapTTypes.add(TransactionType.INVALID);
                    int olapTxnIDOffset = lastTxnId;
                    for (int i = 1; i <= wrkld.getNumTxnTypes(); i++) {
                        String key = "transactiontypes" + workloadTest + "/transactiontype[" + i + "]";
                        String txnName = xmlConfig.getString(key + "/name");
                        int txnId = i;
                        TransactionType tmpType = bench.initTransactionType(txnName, txnId + olapTxnIDOffset);
                        // Keep a reference for filtering
                        activeTXTypes.add(tmpType);

                        // Add a ref for the active TTypes in this benchmark
                        olapTTypes.add(tmpType);
                        lastTxnId = i;
                    }

                    // Wrap the list of transactions and save them
                    TransactionTypes tt = new TransactionTypes(olapTTypes);
                    wrkld.setTransTypes(tt);
                    LOG.debug("Using the following transaction types: " + tt);

                    HashMap<String, List<String>> groupings = new HashMap<String, List<String>>();

                    // even weight to all transactions in the benchmark.
                    List<String> weightAll = new ArrayList<String>();
                    for (int i = 0; i < numTxnTypes; ++i)
                        weightAll.add("1");
                    groupings.put("all", weightAll);
                    benchList.add(bench);
                    //System.out.println("benchList.size = " + benchList.size());

                    // ----------------------------------------------------------------
                    // WORKLOAD CONFIGURATION
                    // ----------------------------------------------------------------

                    SubnodeConfiguration work = xmlConfig.configurationAt("works/work[1]");
                    List<String> weight_strings = null;

                    if (work.containsKey("weights[@workloadType]")) {
                        String weightKey = work.getString("weights" + workloadTest).toLowerCase();
                        weight_strings = getWeights(w, work);
                        //System.out.println("weight_strings[0] = " + weight_strings.get(0)); weight_strings[0] = 100
                    } else {
                        LOG.fatal(String.format("Invalid string for weight. workloadType string must be 'true'"));
                        System.exit(-1);
                    }

                    int rate = 1;
                    boolean rateLimited = true;
                    boolean disabled = false;
                    boolean serial = false;
                    boolean timed = false;

                    // can be "disabled", "unlimited" or a number
                    String rate_string;
                    rate_string = work.getString("rate[not(@workloadType)]", "");
                    rate_string = work.getString("rate" + workloadTest, rate_string);
                    if (rate_string.equals(RATE_DISABLED)) {
                        disabled = true;
                    } else if (rate_string.equals(RATE_UNLIMITED)) {
                        rateLimited = false;
                    } else if (rate_string.isEmpty()) {
                        LOG.fatal(String.format("Please specify the rate for phase %d and workload %s", 1, w));
                        System.exit(-1);
                    } else {
                        try {
                            rate = Integer.parseInt(rate_string);
                            if (rate < 1) {
                                LOG.fatal("Rate limit must be at least 1. Use unlimited or disabled values instead.");
                                System.exit(-1);
                            }
                        } catch (NumberFormatException e) {
                            LOG.fatal(String.format("Rate string must be '%s', '%s' or a number", RATE_DISABLED, RATE_UNLIMITED));
                            System.exit(-1);
                        }
                    }
                    Phase.Arrival arrival = Phase.Arrival.REGULAR;
                    String arrive = work.getString("@arrival", "regular");
                    if (arrive.toUpperCase().equals("POISSON"))
                        arrival = Phase.Arrival.POISSON;

                    // If serial is enabled then run all queries exactly once in serial (rather than
                    // random) order
                    String serial_string;
                    serial_string = work.getString("serial[not(@bench)]", "false");
                    serial_string = work.getString("serial" + pluginTest, serial_string);
                    if (serial_string.equals("true")) {
                        serial = true;
                    } else if (serial_string.equals("false")) {
                        serial = false;
                    } else {
                        LOG.fatal(String.format("Invalid string for serial: '%s'. Serial string must be 'true' or 'false'",
                                serial_string));
                        System.exit(-1);
                    }

                    // We're not actually serial if we're running a script, so make
                    // sure to suppress the serial flag in this case.
                    serial = serial && (wrkld.getTraceReader() == null);

                    int activeTerminals;
                    activeTerminals = work.getInt("active_terminals[not(@bench)]", olapTerminals);
                    activeTerminals = work.getInt("active_terminals" + pluginTest, activeTerminals);
                    if (activeTerminals > (oltpTerminals + olapTerminals)) {
                        LOG.error(String.format("Configuration error in work %d: "
                                + "Number of active terminals is bigger than the total number of (oltpTerminals+olapTerminals)", 1));
                        System.exit(-1);
                    }

                    int time = work.getInt("/time", 0);
                    int warmup = work.getInt("/warmup", 0);
                    timed = (time > 0);

                    //System.out.println("!timed: " + !timed + "  serial : " + serial);
                    if (scriptRun) {
                        LOG.info("Running a script; ignoring timer, serial, and weight settings.");
                    } else if (!timed) {
                        if (serial) {
                            if (activeTerminals > 1) {
                                // For serial executions, we usually want only one terminal, but not always!
                                // (e.g. the CHBenCHmark)
                                LOG.warn("\n" + StringBoxUtil.heavyBox(String.format(
                                        "WARNING: Serial execution is enabled but the number of active terminals[=%d] > 1.\nIs this intentional??",
                                        activeTerminals)));
                            }
                            LOG.info("Timer disabled for serial run; will execute"
                                    + " all queries exactly once.");
                        } else {
                            LOG.fatal("Must provide positive time bound for"
                                    + " non-serial executions. Either provide"
                                    + " a valid time or enable serial mode.");
                            System.exit(-1);
                        }
                    } else if (serial)
                        LOG.info("Timer enabled for serial run; will run queries"
                                + " serially in a loop until the timer expires.");
                    if (warmup < 0) {
                        LOG.fatal("Must provide nonnegative time bound for"
                                + " warmup.");
                        System.exit(-1);
                    }

                    wrkld.addWork(time,
                            warmup,
                            rate,
                            weight_strings,
                            rateLimited,
                            disabled,
                            serial,
                            timed,
                            activeTerminals,
                            arrival);

                    // CHECKING INPUT PHASES
/*                    int j = 0;
                    for (Phase p : wrkld.getAllPhases()) {
                        j++;
                        if (p.getWeightCount() != wrkld.getOlapTxnTypes()) {
                            LOG.fatal(String.format("Configuration files is inconsistent, phase %d contains %d weights but you defined %d transaction types",
                                    j, p.getWeightCount(), wrkld.getOlapTxnTypes()));
                            if (p.isSerial()) {
                                LOG.fatal("However, note that since this a serial phase, the weights are irrelevant (but still must be included---sorry).");
                            }
                            System.exit(-1);
                        }
                    }*/ // FOR


                } else if(w.equals("olxp")){
                    //System.out.println("workloadTest = " + workloadTest); workloadTest = [@workloadType='oltp']
                    wrkld.setNumTxnTypes(numTxnTypes);
                    List<TransactionType> olxpTTypes = new ArrayList<TransactionType>();
                    olxpTTypes.add(TransactionType.INVALID);
                    int olxpTxnIDOffset = lastTxnId;
                    for (int i = 1; i <= wrkld.getNumTxnTypes(); i++) {
                        String key = "transactiontypes" + workloadTest + "/transactiontype[" + i + "]";
                        String txnName = xmlConfig.getString(key + "/name");
                        int txnId = i;
                        TransactionType tmpType = bench.initTransactionType(txnName, txnId + olxpTxnIDOffset);
                        // Keep a reference for filtering
                        activeTXTypes.add(tmpType);

                        // Add a ref for the active TTypes in this benchmark
                        olxpTTypes.add(tmpType);
                        lastTxnId = i;
                    }

                    // Wrap the list of transactions and save them
                    TransactionTypes tt = new TransactionTypes(olxpTTypes);
                    wrkld.setTransTypes(tt);
                    LOG.debug("Using the following transaction types: " + tt);

                    HashMap<String, List<String>> groupings = new HashMap<String, List<String>>();

                    // even weight to all transactions in the benchmark.
                    List<String> weightAll = new ArrayList<String>();
                    for (int i = 0; i < numTxnTypes; ++i)
                        weightAll.add("1");
                    groupings.put("all", weightAll);
                    benchList.add(bench);

                    // ----------------------------------------------------------------
                    // WORKLOAD CONFIGURATION
                    // ----------------------------------------------------------------

                    SubnodeConfiguration work = xmlConfig.configurationAt("works/work[1]");
                    List<String> olxp_weight_strings = null;

                    if (work.containsKey("weights[@workloadType]")) {
                        String weightKey = work.getString("weights" + workloadTest).toLowerCase();
                        olxp_weight_strings = getWeights(w, work);
                        //System.out.println("weight_strings[0] = " + weight_strings.get(0)); weight_strings[0] = 45
                    } else {
                        LOG.fatal(String.format("Invalid string for weight. workloadType string must be 'true'"));
                        System.exit(-1);
                    }

                    int rate = 1;
                    boolean rateLimited = true;
                    boolean disabled = false;
                    boolean serial = false;
                    boolean timed = false;

                    // can be "disabled", "unlimited" or a number
                    String rate_string;
                    rate_string = work.getString("rate[not(@workloadType)]", "");
                    rate_string = work.getString("rate" + workloadTest, rate_string);
                    if (rate_string.equals(RATE_DISABLED)) {
                        disabled = true;
                    } else if (rate_string.equals(RATE_UNLIMITED)) {
                        rateLimited = false;
                    } else if (rate_string.isEmpty()) {
                        LOG.fatal(String.format("Please specify the rate for phase %d and workload %s", 1, w));
                        System.exit(-1);
                    } else {
                        try {
                            rate = Integer.parseInt(rate_string);
                            if (rate < 1) {
                                LOG.fatal("Rate limit must be at least 1. Use unlimited or disabled values instead.");
                                System.exit(-1);
                            }
                        } catch (NumberFormatException e) {
                            LOG.fatal(String.format("Rate string must be '%s', '%s' or a number", RATE_DISABLED, RATE_UNLIMITED));
                            System.exit(-1);
                        }
                    }
                    Phase.Arrival arrival = Phase.Arrival.REGULAR;
                    String arrive = work.getString("@arrival", "regular");
                    if (arrive.toUpperCase().equals("POISSON"))
                        arrival = Phase.Arrival.POISSON;

                    // If serial is enabled then run all queries exactly once in serial (rather than
                    // random) order
                    String serial_string;
                    serial_string = work.getString("serial[not(@bench)]", "false");
                    serial_string = work.getString("serial" + pluginTest, serial_string);
                    if (serial_string.equals("true")) {
                        serial = true;
                    } else if (serial_string.equals("false")) {
                        serial = false;
                    } else {
                        LOG.fatal(String.format("Invalid string for serial: '%s'. Serial string must be 'true' or 'false'",
                                serial_string));
                        System.exit(-1);
                    }

                    // We're not actually serial if we're running a script, so make
                    // sure to suppress the serial flag in this case.
                    serial = serial && (wrkld.getTraceReader() == null);

                    int activeTerminals;
                    activeTerminals = work.getInt("active_terminals[not(@bench)]", olxpTerminals);
                    activeTerminals = work.getInt("active_terminals" + pluginTest, activeTerminals);
                    if (activeTerminals > (olxpTerminals)) {
                        LOG.error(String.format("Configuration error in work %d: "
                                + "Number of active terminals is bigger than the total number of (olxpTerminals)", 1));
                        System.exit(-1);
                    }

                    int time = work.getInt("/time", 0);
                    int warmup = work.getInt("/warmup", 0);
                    timed = (time > 0);

                    // System.out.println("!timed: " + !timed + "  serial : " + serial);

                    if (scriptRun) {
                        LOG.info("Running a script; ignoring timer, serial, and weight settings.");
                    } else if (!timed) {
                        if (serial) {
                            if (activeTerminals > 1) {
                                // For serial executions, we usually want only one terminal, but not always!
                                // (e.g. the CHBenCHmark)
                                LOG.warn("\n" + StringBoxUtil.heavyBox(String.format(
                                        "WARNING: Serial execution is enabled but the number of active terminals[=%d] > 1.\nIs this intentional??",
                                        activeTerminals)));
                            }
                            LOG.info("Timer disabled for serial run; will execute"
                                    + " all queries exactly once.");
                        } else {
                            LOG.fatal("Must provide positive time bound for"
                                    + " non-serial executions. Either provide"
                                    + " a valid time or enable serial mode.");
                            System.exit(-1);
                        }
                    } else if (serial)
                        LOG.info("Timer enabled for serial run; will run queries"
                                + " serially in a loop until the timer expires.");
                    if (warmup < 0) {
                        LOG.fatal("Must provide nonnegative time bound for"
                                + " warmup.");
                        System.exit(-1);
                    }

                    wrkld.addWork(time,
                            warmup,
                            rate,
                            olxp_weight_strings,
                            rateLimited,
                            disabled,
                            serial,
                            timed,
                            activeTerminals,
                            arrival);
                }


                // Generate the dialect map
                wrkld.init();

                assert (wrkld.getNumTxnTypes() >= 0);
                assert (xmlConfig != null);
            //}
        }
        assert(benchList.isEmpty() == false);
        assert(benchList.get(0) != null);
        
        // Export StatementDialects
        if (isBooleanOptionSet(argsLine, "dialects-export")) {
            BenchmarkModule bench = benchList.get(0);
            if (bench.getStatementDialects() != null) {
                LOG.info("Exporting StatementDialects for " + bench);
                String xml = bench.getStatementDialects().export(bench.getWorkloadConfiguration().getDBType(),
                                                                 bench.getProcedures().values());
                System.out.println(xml);
                System.exit(0);
            }
            throw new RuntimeException("No StatementDialects is available for " + bench);
        }

        
        @Deprecated
        boolean verbose = argsLine.hasOption("v");

        // Create the Benchmark's Database
        if (isBooleanOptionSet(argsLine, "create")) {
            for (BenchmarkModule benchmark : benchList) {
                LOG.info("Creating new " + benchmark.getBenchmarkName().toUpperCase() + " database...");
                runCreator(benchmark, verbose);
                LOG.info("Finished!");
                LOG.info(SINGLE_LINE);
            }
        } else if (LOG.isDebugEnabled()) {
            LOG.debug("Skipping creating benchmark database tables");
            LOG.info(SINGLE_LINE);
        }

        // Clear the Benchmark's Database
        if (isBooleanOptionSet(argsLine, "clear")) {
                for (BenchmarkModule benchmark : benchList) {
                LOG.info("Resetting " + benchmark.getBenchmarkName().toUpperCase() + " database...");
                benchmark.clearDatabase();
                LOG.info("Finished!");
                LOG.info(SINGLE_LINE);
            }
        } else if (LOG.isDebugEnabled()) {
            LOG.debug("Skipping creating benchmark database tables");
            LOG.info(SINGLE_LINE);
        }

        // Execute Loader
        if (isBooleanOptionSet(argsLine, "load")) {
            for (BenchmarkModule benchmark : benchList) {
                LOG.info(String.format("Loading data into %s database with %d threads...",
                                       benchmark.getBenchmarkName().toUpperCase(),
                                       benchmark.getWorkloadConfiguration().getLoaderThreads()));
                runLoader(benchmark, verbose);
                LOG.info("Finished!");
                LOG.info(SINGLE_LINE);
            }
        } else if (LOG.isDebugEnabled()) {
            LOG.debug("Skipping loading benchmark database records");
            LOG.info(SINGLE_LINE);
        }
        
        // Execute a Script
        if (argsLine.hasOption("runscript")) {
            for (BenchmarkModule benchmark : benchList) {
                String script = argsLine.getOptionValue("runscript");
                LOG.info("Running a SQL script: "+script);
                runScript(benchmark, script);
                LOG.info("Finished!");
                LOG.info(SINGLE_LINE);
            }
        }

        // Execute Workload
        if (isBooleanOptionSet(argsLine, "execute")) {
            List<Results> Result = new ArrayList<Results>();
            //System.out.println("benchList size are : " + benchList.size());
            int i = 0;
            int j = 0;
            for (String workloadType : workloads) {
                try {
                    Result.add(runWorkload(benchList.get(i++), verbose, intervalMonitor, workloadType));
                } catch (Throwable ex) {
                    LOG.error("Unexpected error when running benchmarks.", ex);
                    System.exit(1);
                }
                assert(Result.get(i) != null);
                writeOutputs(Result.get(j++), activeTXTypes, argsLine, xmlConfig);
            }
        } else {
            LOG.info("Skipping benchmark workload execution");
        }
    }
    
    private static String writeHistograms(Results r) {
        StringBuilder sb = new StringBuilder();
        
        sb.append(StringUtil.bold("Completed Transactions:"))
          .append("\n")
          .append(r.getTransactionSuccessHistogram())
          .append("\n\n");
        
        sb.append(StringUtil.bold("Aborted Transactions:"))
          .append("\n")
          .append(r.getTransactionAbortHistogram())
          .append("\n\n");
        
        sb.append(StringUtil.bold("Rejected Transactions (Server Retry):"))
          .append("\n")
          .append(r.getTransactionRetryHistogram())
          .append("\n\n");
        
        sb.append(StringUtil.bold("Unexpected Errors:"))
          .append("\n")
          .append(r.getTransactionErrorHistogram());
        
        if (r.getTransactionAbortMessageHistogram().isEmpty() == false)
            sb.append("\n\n")
              .append(StringUtil.bold("User Aborts:"))
              .append("\n")
              .append(r.getTransactionAbortMessageHistogram());
        
        return (sb.toString());
    }
    
    private static String writeJSONHistograms(Results r) {
        Map<String, JSONSerializable> map = new HashMap<>();
        map.put("completed", r.getTransactionSuccessHistogram());
        map.put("aborted", r.getTransactionAbortHistogram());
        map.put("rejected", r.getTransactionRetryHistogram());
        map.put("unexpected", r.getTransactionErrorHistogram());
        return JSONUtil.toJSONString(map);
    }
    
        
    /**
     * Write out the results for a benchmark run to a bunch of files
     * @param r
     * @param activeTXTypes
     * @param argsLine
     * @param xmlConfig
     * @throws Exception
     */
    private static void writeOutputs(Results r, List<TransactionType> activeTXTypes, CommandLine argsLine, XMLConfiguration xmlConfig) throws Exception {
        
        // If an output directory is used, store the information
        String outputDirectory = "results";
        if (argsLine.hasOption("d")) {
            outputDirectory = argsLine.getOptionValue("d");
        }
        String filePrefix = "";
        if (argsLine.hasOption("t")) {
            filePrefix = String.valueOf(TimeUtil.getCurrentTime().getTime()) + "_";
        }
        
        // Special result uploader
        ResultUploader ru = null;
        if (xmlConfig.containsKey("uploadUrl")) {
            ru = new ResultUploader(r, xmlConfig, argsLine);
            LOG.info("Upload Results URL: " + ru);
        }
        
        // Output target 
        PrintStream ps = null;
        PrintStream rs = null;
        String baseFileName = "oltpbench";
        if (argsLine.hasOption("o")) {
            if (argsLine.getOptionValue("o").equals("-")) {
                ps = System.out;
                rs = System.out;
                baseFileName = null;
            } else {
                baseFileName = argsLine.getOptionValue("o");
            }
        }

        // Build the complex path
        String baseFile = filePrefix;
        String nextName;
        
        if (baseFileName != null) {
            // Check if directory needs to be created
            if (outputDirectory.length() > 0) {
                FileUtil.makeDirIfNotExists(outputDirectory.split("/"));
            }
            
            baseFile = filePrefix + baseFileName;

            if (argsLine.getOptionValue("output-raw", "true").equalsIgnoreCase("true")) {
                // RAW OUTPUT
                nextName = FileUtil.getNextFilename(FileUtil.joinPath(outputDirectory, baseFile + ".csv"));
                rs = new PrintStream(new File(nextName));
                LOG.info("Output Raw data into file: " + nextName);
                r.writeAllCSVAbsoluteTiming(activeTXTypes, rs);
                rs.close();
            }

            if (isBooleanOptionSet(argsLine, "output-samples")) {
                // Write samples using 1 second window
                nextName = FileUtil.getNextFilename(FileUtil.joinPath(outputDirectory, baseFile + ".samples"));
                rs = new PrintStream(new File(nextName));
                LOG.info("Output samples into file: " + nextName);
                r.writeCSV2(rs);
                rs.close();
            }

            // Result Uploader Files
            if (ru != null) {
                // Summary Data
                nextName = FileUtil.getNextFilename(FileUtil.joinPath(outputDirectory, baseFile + ".summary"));
                PrintStream ss = new PrintStream(new File(nextName));
                LOG.info("Output summary data into file: " + nextName);
                ru.writeSummary(ss);
                ss.close();

                // DBMS Parameters
                nextName = FileUtil.getNextFilename(FileUtil.joinPath(outputDirectory, baseFile + ".params"));
                ss = new PrintStream(new File(nextName));
                LOG.info("Output DBMS parameters into file: " + nextName);
                ru.writeDBParameters(ss);
                ss.close();

                // DBMS Metrics
                nextName = FileUtil.getNextFilename(FileUtil.joinPath(outputDirectory, baseFile + ".metrics"));
                ss = new PrintStream(new File(nextName));
                LOG.info("Output DBMS metrics into file: " + nextName);
                ru.writeDBMetrics(ss);
                ss.close();

                // Experiment Configuration
                nextName = FileUtil.getNextFilename(FileUtil.joinPath(outputDirectory, baseFile + ".expconfig"));
                ss = new PrintStream(new File(nextName));
                LOG.info("Output experiment config into file: " + nextName);
                ru.writeBenchmarkConf(ss);
                ss.close();
                //everyadd
                //Thread.sleep(30000);
                //everyadd
           }
            
        } else if (LOG.isDebugEnabled()) {
            LOG.debug("No output file specified");
        }
        
        if (isBooleanOptionSet(argsLine, "upload") && ru != null) {
            ru.uploadResult(activeTXTypes);
        }
        
        // SUMMARY FILE
        if (argsLine.hasOption("s")) {
            nextName = FileUtil.getNextFilename(FileUtil.joinPath(outputDirectory, baseFile + ".res"));
            ps = new PrintStream(new File(nextName));
            LOG.info("Output throughput samples into file: " + nextName);
            
            int windowSize = Integer.parseInt(argsLine.getOptionValue("s"));
            LOG.info("Grouped into Buckets of " + windowSize + " seconds");
            r.writeCSV(windowSize, ps);

            // Allow more detailed reporting by transaction to make it easier to check
            if (argsLine.hasOption("ss")) {
                
                for (TransactionType t : activeTXTypes) {
                    PrintStream ts = ps;
                    if (ts != System.out) {
                        // Get the actual filename for the output
                        baseFile = filePrefix + baseFileName + "_" + t.getName();
                        nextName = FileUtil.getNextFilename(FileUtil.joinPath(outputDirectory, baseFile + ".res"));                            
                        ts = new PrintStream(new File(nextName));
                        r.writeCSV(windowSize, ts, t);
                        ts.close();
                    }
                }
            }
        } else if (LOG.isDebugEnabled()) {
            LOG.warn("No bucket size specified");
        }
        
        // WRITE HISTOGRAMS
        if (argsLine.hasOption("histograms")) {
            String histogram_result = writeHistograms(r);
            LOG.info(SINGLE_LINE);
            LOG.info("Workload Histograms:\n" + histogram_result);
            LOG.info(SINGLE_LINE);
        }
        if (argsLine.hasOption("json-histograms")) {
            String histogram_json = writeJSONHistograms(r);
            String fileName = argsLine.getOptionValue("json-histograms");
            FileUtil.writeStringToFile(new File(fileName), histogram_json);
            LOG.info("Histograms JSON Data: " + fileName);
        }
        
        
        if (ps != null) ps.close();
        if (rs != null) rs.close();
    }

    /* buggy piece of shit of Java XPath implementation made me do it 
       replaces good old [@bench="{plugin_name}", which doesn't work in Java XPath with lists
     */
    private static List<String> getWeights(String plugin, SubnodeConfiguration work) {

        List<String> weight_strings = new LinkedList<String>();
        @SuppressWarnings("unchecked")
        List<SubnodeConfiguration> weights = work.configurationsAt("weights");
        boolean weights_started = false;

        for (SubnodeConfiguration weight : weights) {

            // stop if second attributed node encountered
            if (weights_started && weight.getRootNode().getAttributeCount() > 0) {
                break;
            }
            // start adding node values, if node with attribute equal to current
            // plugin encountered
            if (weight.getRootNode().getAttributeCount() > 0 && weight.getRootNode().getAttribute(0).getValue().equals(plugin)) {
                weights_started = true;
            }
            if (weights_started) {
                weight_strings.add(weight.getString(""));
            }

        }
        return weight_strings;
    }

    //everyadd
    private static List<String> getOlapWeights(String plugin, SubnodeConfiguration work) {

        List<String> weight_strings = new LinkedList<String>();
        @SuppressWarnings("unchecked")
        List<SubnodeConfiguration> weights = work.configurationsAt("weights");
        boolean weights_started = false;

        for (SubnodeConfiguration weight : weights) {

            // stop if second attributed node encountered
            if (weights_started && weight.getRootNode().getAttributeCount() > 0) {
                break;
            }
            // start adding node values, if node with attribute equal to current
            // plugin encountered
            if (weight.getRootNode().getAttributeCount() > 0 && weight.getRootNode().getAttribute(0).getValue().equals(plugin)) {
                weights_started = true;
            }
            if (weights_started) {
                weight_strings.add(weight.getString(""));
            }

        }
        return weight_strings;
    }
    //everyadd
    
    private static void runScript(BenchmarkModule bench, String script) {
        LOG.debug(String.format("Running %s", script));
        bench.runScript(script);
    }

    private static void runCreator(BenchmarkModule bench, boolean verbose) {
        LOG.debug(String.format("Creating %s Database", bench));
        bench.createDatabase();
    }
    
    private static void runLoader(BenchmarkModule bench, boolean verbose) {
        LOG.debug(String.format("Loading %s Database", bench));
        bench.loadDatabase();
    }

        private static Results runWorkload(BenchmarkModule bench, boolean verbose, int intervalMonitor, String workloadType) throws QueueLimitException, IOException {
        List<Worker<?>> workers = new ArrayList<Worker<?>>();
        List<WorkloadConfiguration> workConfs = new ArrayList<WorkloadConfiguration>();

        //BenchmarkModule bench = benchList.get(0);
        //System.out.println("The first bench is " + bench.getBenchmarkName());

        if(workloadType.equals("oltp")) {
            LOG.info("Creating " + bench.getWorkloadConfiguration().getOltpTerminals() + " virtual oltpTerminals...");
            workers.addAll(bench.makeWorkers(verbose, workloadType));

            int num_phases = bench.getWorkloadConfiguration().getNumberOfPhases();
            LOG.info(String.format("Launching the %s Benchmark with %s Phase%s...",
                    bench.getBenchmarkName().toUpperCase(), num_phases, (num_phases > 1 ? "s" : "")));
            workConfs.add(bench.getWorkloadConfiguration());
        }

        if(workloadType.equals("olap")) {
                LOG.info("Creating " + bench.getWorkloadConfiguration().getOlapTerminals() + " virtual olapTerminals...");
                workers.addAll(bench.makeWorkers(verbose, workloadType));

                int num_phases = bench.getWorkloadConfiguration().getNumberOfPhases();
                LOG.info(String.format("Launching the %s Benchmark with %s Phase%s...",
                        bench.getBenchmarkName().toUpperCase(), num_phases, (num_phases > 1 ? "s" : "")));
                workConfs.add(bench.getWorkloadConfiguration());
            }

            if(workloadType.equals("olxp")) {
                LOG.info("Creating " + bench.getWorkloadConfiguration().getOlxpTerminals() + " virtual olxpTerminals...");
                workers.addAll(bench.makeWorkers(verbose, workloadType));

                int num_phases = bench.getWorkloadConfiguration().getNumberOfPhases();
                LOG.info(String.format("Launching the %s Benchmark with %s Phase%s...",
                        bench.getBenchmarkName().toUpperCase(), num_phases, (num_phases > 1 ? "s" : "")));
                workConfs.add(bench.getWorkloadConfiguration());
            }


            Results r = ThreadBench.runRateLimitedBenchmark(workers, workConfs, intervalMonitor);
            LOG.info(SINGLE_LINE);
            LOG.info("Rate limited reqs/s: " + r);
            return r;
    }

    private static void printUsage(Options options) {
        HelpFormatter hlpfrmt = new HelpFormatter();
        hlpfrmt.printHelp("oltpbenchmark", options);
    }

    /**
     * Returns true if the given key is in the CommandLine object and is set to
     * true.
     * 
     * @param argsLine
     * @param key
     * @return
     */
    private static boolean isBooleanOptionSet(CommandLine argsLine, String key) {
        if (argsLine.hasOption(key)) {
            LOG.debug("CommandLine has option '" + key + "'. Checking whether set to true");
            String val = argsLine.getOptionValue(key);
            LOG.debug(String.format("CommandLine %s => %s", key, val));
            return (val != null ? val.equalsIgnoreCase("true") : false);
        }
        return (false);
    }
    
    public static String getAssertWarning() {
        String msg = "!!! WARNING !!!\n" +
                     "OLTP-Bench is executing with JVM asserts enabled. This will degrade runtime performance.\n" +
                     "You can disable them by setting the config option 'assertions' to FALSE";
        return StringBoxUtil.heavyBox(msg);
    }
}
