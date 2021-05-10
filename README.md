# Extra-Credit-Part-2

data profiling: ``CountRecs.java, CountRecsMapper.java, CountRecsReducer.java``

It's a typical count line MapReduce job. In CountRecsMapper and CountRecsReducer, it counts the number of records (lines), and CountRecs is the runner to run the program. 



ETL: ``clean.java, cleanUsers.java``

In cleanUsers, I removed records with null or exceptions, as well as useless columns. Since I only need ( id BIGINT, login STRING, created_at String, type STRING, fake STRING, deleted STRING, country_code STRING) to evaluate the projects, I only keep Strings whose index is 0, 1, 3, 4, 5, 6, or 9. In clean, we use it to run the program.
