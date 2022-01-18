# OLxPBench

Together with the framework we provide the following HTAP benchmarks:
  * Subenchmark
  * Fibenchmark
  * Tabenchmark

## Dependencies

+ Java (+1.7)
+ Apache Ant

## Download
 + git clone https://github.com/EVERYGO111/olxpbench.git

## Quick start
+ ant bootstrap
+ ant resolve
+ ant build

## Config file
The ./config directory provides all configure files.

## Example

+ ./olxpbenchmark -b subenchmark -c config/suoltp.xml -wt oltp --create=true --load=true
+ ./olxpbenchmark -b subenchmark -c config/suoltp.xml -wt oltp --execute=true -o results/suoltp

## Troubles with TiDB
isolation level error
+ set GLOBAL tidb_skip_isolation_level_check = 1
+ 

java.sql.SQLException: Write conflict
+ SET GLOBAL tidb_txn_mode = 'pessimistic';
+ 

MySQLSyntaxErrorException:this is incompatible with sql_mode=only_full_group_by
+ set @@global.sql_mode ='STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION';

