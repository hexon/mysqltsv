# mysqltsv

A simple Go library to encode values into a TSV file (tab separated values) for
MySQL's LOAD DATA INFILE.

More information can be found at
https://dev.mysql.com/doc/refman/8.0/en/load-data.html#load-data-field-line-handling

## Character sets

Characters sets are the worst. Make sure to verify your data is loaded correctly before relying on this not to corrupt your data.
