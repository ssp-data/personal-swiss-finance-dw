# Personal Finance Data Warehouse

This project is used to categorize my finances.

I exported my statements in CSVs and reading them in with dagster and model the data with dbt.
Rill data is used for dasboards.


--- 

There are some older code for downloading and exporing BEKB format t940 directly via web. But I moved them to `_archive` now. Might add it later again to the main project. The features it had was:
* to extract transaction information from an t940-export (old swiss format) export to a JSON


