# OLxPBench

Together with the framework we provide the following HTAP benchmarks:
  * Subenchmark
  * Fibenchmark
  * Tabenchmark
  
## Dependencies

+ Java (+1.7)
+ Apache Ant
+ Ubuntu (+16.04)

## Download
 + git clone https://github.com/BenchCouncil/olxpbench

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


java.sql.SQLException: Write conflict
+ SET GLOBAL tidb_txn_mode = 'pessimistic';


MySQLSyntaxErrorException:this is incompatible with sql_mode=only_full_group_by
+ set @@global.sql_mode ='STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION';

## Publications
If you use OLxPBench for your paper, please cite the follow paper:
OLxPBench: Real-time, Semantically Consistent, and Domain-specific are Essential in Benchmarking, Designing, and Implementing HTAP Systems (ICDE 2022)

@misc{2203.16095,
Author = {Guoxin Kang and Lei Wang and Wanling Gao and Fei Tang and Jianfeng Zhan},
Title = {OLxPBench: Real-time, Semantically Consistent, and Domain-specific are Essential in Benchmarking, Designing, and Implementing HTAP Systems},
Year = {2022},
Eprint = {arXiv:2203.16095},
}
