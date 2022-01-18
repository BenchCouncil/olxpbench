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
