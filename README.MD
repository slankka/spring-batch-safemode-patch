# Spring Batch/Cloud Task safemode patch

This patch is aim to solve a Exception that database with a safe mode will cause updating `seq` table failed.

Take a look at this project, life will be easier.

```
Could not increment ID for BATCH_JOB_SEQ sequence table;
nested exception is java.sql.SQLException:
You are using safe update mode and you tried to update a table without a WHERE that uses a KEY column
```

The patch files are in `io.github.slankka.springbatch.safemode.patch` package.


# Usage
you can copy the code in `io.github.slankka.springbatch.safemode.patch`.
then add to your SpringBatch or Spring Cloud Task project as a module.
Then add this as module dependency to your main module.

# Examples

* package `io.github.slankka.springbatch.safemode.springbatchexample` is a example for spring batch project. The most important code is 
`io.github.slankka.springbatch.safemode.springbatchexample.SafeModeBatchConfigurer#createJobRepository`

* package `io.github.slankka.springbatch.safemode.springcloudtaskexample` is for spring cloud task project.

# Difference between Spring Cloud Task and Spring Batch
Spring Cloud Task is build by Spring Batch, But it is designed to run a short-life program.
Spring Cloud Task application using `@EnableTask` annotation. It
uses `TaskConfigurer` (default is `org.springframework.cloud.task.configuration.DefaultTaskConfigurer`) to configure its components.

Spring Batch can build a web-based batch job server.The bootstrap class annotated with `@EnableBatchProcessing`
Spring batch using `BatchConfigurer`(default is `org.springframework.batch.core.configuration.annotation.DefaultBatchConfigurer`)
to configure its `JobRepository`, `JobExplorer`, etc.





