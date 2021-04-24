package io.github.slankka.springbatch.safemode.springbatchexample;

import io.github.slankka.springbatch.safemode.patch.SafeModeMysqlIncrementFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.configuration.BatchConfigurationException;
import org.springframework.batch.core.configuration.annotation.BatchConfigurer;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.explore.support.JobExplorerFactoryBean;
import org.springframework.batch.core.explore.support.MapJobExplorerFactoryBean;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;

@Component
public class SafeModeBatchConfigurer implements BatchConfigurer {
    private static final Log logger = LogFactory.getLog(SafeModeBatchConfigurer.class);
    private DataSource dataSource;
    private PlatformTransactionManager transactionManager;
    private JobRepository jobRepository;
    private JobLauncher jobLauncher;
    private JobExplorer jobExplorer;


    protected SafeModeBatchConfigurer() {
    }

    @Autowired
    public SafeModeBatchConfigurer(@Qualifier("datasource") DataSource dataSource,
                                   @Qualifier("appAsyncJobLauncher") JobLauncher appAsyncJobLauncher) {
        this.jobLauncher = appAsyncJobLauncher;
        setDataSource(dataSource);
    }

    /**
     * Sets the dataSource.  If the {@link DataSource} has been set once, all future
     * values are passed are ignored (to prevent {@code}@Autowired{@code} from overwriting
     * the value).
     *
     * @param dataSource
     */
    @Autowired(required = false)
    public void setDataSource(DataSource dataSource) {
        if (this.dataSource == null) {
            this.dataSource = dataSource;
        }

        if (getTransactionManager() == null) {
            logger.warn("No transaction manager was provided, using a DataSourceTransactionManager");
            this.transactionManager = new DataSourceTransactionManager(this.dataSource);
        }
    }


    @Override
    public JobRepository getJobRepository() {
        return jobRepository;
    }

    @Override
    public PlatformTransactionManager getTransactionManager() {
        return transactionManager;
    }

    @Override
    public JobLauncher getJobLauncher() {
        return jobLauncher;
    }

    @Override
    public JobExplorer getJobExplorer() {
        return jobExplorer;
    }

    @PostConstruct
    public void initialize() {
        try {
            if (dataSource == null) {
                logger.warn("No datasource was provided...using a Map based JobRepository");

                if (getTransactionManager() == null) {
                    logger.warn("No transaction manager was provided, using a ResourcelessTransactionManager");
                    this.transactionManager = new ResourcelessTransactionManager();
                }

                MapJobRepositoryFactoryBean jobRepositoryFactory = new MapJobRepositoryFactoryBean(getTransactionManager());
                jobRepositoryFactory.afterPropertiesSet();
                this.jobRepository = jobRepositoryFactory.getObject();

                MapJobExplorerFactoryBean jobExplorerFactory = new MapJobExplorerFactoryBean(jobRepositoryFactory);
                jobExplorerFactory.afterPropertiesSet();
                this.jobExplorer = jobExplorerFactory.getObject();
            } else {
                this.jobRepository = createJobRepository();
                this.jobExplorer = createJobExplorer();
            }

        } catch (Exception e) {
            throw new BatchConfigurationException(e);
        }
    }

    /**
     * The key step: change the DefaultMysqlIncreamentFactory to SafeModeMysqlIncreamentFactory
     */
    protected JobRepository createJobRepository() throws Exception {
        JobRepositoryFactoryBean factory = new JobRepositoryFactoryBean();
        factory.setIncrementerFactory(new SafeModeMysqlIncrementFactory(dataSource));
        factory.setDataSource(dataSource);
        factory.setTransactionManager(getTransactionManager());
        factory.afterPropertiesSet();
        return factory.getObject();
    }

    protected JobExplorer createJobExplorer() throws Exception {
        JobExplorerFactoryBean jobExplorerFactoryBean = new JobExplorerFactoryBean();
        jobExplorerFactoryBean.setDataSource(this.dataSource);
        jobExplorerFactoryBean.afterPropertiesSet();
        return jobExplorerFactoryBean.getObject();
    }

}