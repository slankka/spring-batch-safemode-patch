package io.github.slankka.springbatch.safemode.springbatchexample;

import org.springframework.batch.core.configuration.BatchConfigurationException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration
public class AppConfig {

    @Value("${task.core.pool.size:50}")
    private int taskCorePoolSize;

    @Value("${task.max.pool.size:100}")
    private int taskMaxPoolSize;

    @Value("${job.core.pool.size:50}")
    private int jobCorePoolSize;

    @Value("${job.max.pool.size:100}")
    private int jobMaxPoolSize;

    @Bean
    public TaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setThreadNamePrefix("tsk-Exec-");
        executor.setMaxPoolSize(taskMaxPoolSize);
        executor.setCorePoolSize(taskCorePoolSize);
        return executor;
    }

    @Bean("appAsyncJobLauncher")
    public JobLauncher jobLauncher(JobRepository jobRepository) {
        ThreadPoolTaskExecutor simpleAsyncTaskExecutor = new ThreadPoolTaskExecutor();
        simpleAsyncTaskExecutor.setThreadNamePrefix("job-Exec-");
        simpleAsyncTaskExecutor.setCorePoolSize(jobCorePoolSize);
        simpleAsyncTaskExecutor.setMaxPoolSize(jobMaxPoolSize);
        simpleAsyncTaskExecutor.afterPropertiesSet();
        try {
            SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
            jobLauncher.setJobRepository(jobRepository);
            jobLauncher.setTaskExecutor(simpleAsyncTaskExecutor);
            jobLauncher.afterPropertiesSet();
            return jobLauncher;
        } catch (Exception e) {
            throw new BatchConfigurationException(e);
        }
    }
}
