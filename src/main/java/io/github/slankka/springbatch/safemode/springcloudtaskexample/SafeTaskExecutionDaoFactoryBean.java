package io.github.slankka.springbatch.safemode.springcloudtaskexample;

import io.github.slankka.springbatch.safemode.patch.SafeModeMysqlIncreamentFactory;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.cloud.task.configuration.TaskProperties;
import org.springframework.cloud.task.repository.dao.JdbcTaskExecutionDao;
import org.springframework.cloud.task.repository.dao.MapTaskExecutionDao;
import org.springframework.cloud.task.repository.dao.TaskExecutionDao;
import org.springframework.cloud.task.repository.support.DatabaseType;
import org.springframework.jdbc.support.MetaDataAccessException;
import org.springframework.util.Assert;

import javax.sql.DataSource;

public class SafeTaskExecutionDaoFactoryBean implements FactoryBean<TaskExecutionDao> {

    private DataSource dataSource;

    private TaskExecutionDao dao = null;

    private String tablePrefix = TaskProperties.DEFAULT_TABLE_PREFIX;

    /**
     * Default constructor will result in a Map based TaskExecutionDao.  <b>This is only
     * intended for testing purposes.</b>
     */
    public SafeTaskExecutionDaoFactoryBean() {
    }

    /**
     * {@link DataSource} to be used.
     *
     * @param dataSource  {@link DataSource} to be used.
     * @param tablePrefix the table prefix to use for this dao.
     */
    public SafeTaskExecutionDaoFactoryBean(DataSource dataSource, String tablePrefix) {
        this(dataSource);
        Assert.hasText(tablePrefix, "tablePrefix must not be null nor empty");
        this.tablePrefix = tablePrefix;
    }

    /**
     * {@link DataSource} to be used.
     *
     * @param dataSource {@link DataSource} to be used.
     */
    public SafeTaskExecutionDaoFactoryBean(DataSource dataSource) {
        Assert.notNull(dataSource, "A DataSource is required");

        this.dataSource = dataSource;
    }

    @Override
    public TaskExecutionDao getObject() throws Exception {
        if (this.dao == null) {
            if (this.dataSource != null) {
                buildTaskExecutionDao(this.dataSource);
            } else {
                this.dao = new MapTaskExecutionDao();
            }
        }

        return this.dao;
    }

    @Override
    public Class<?> getObjectType() {
        return TaskExecutionDao.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    private void buildTaskExecutionDao(DataSource dataSource) {
        SafeModeMysqlIncreamentFactory incrementerFactory = new SafeModeMysqlIncreamentFactory(dataSource);
        this.dao = new JdbcTaskExecutionDao(dataSource, this.tablePrefix);
        String databaseType;
        try {
            databaseType = DatabaseType.fromMetaData(dataSource).name();
        } catch (MetaDataAccessException e) {
            throw new IllegalStateException(e);
        }
        ((JdbcTaskExecutionDao) this.dao).setTaskIncrementer(incrementerFactory.getIncrementer(databaseType, this.tablePrefix + "SEQ"));
    }
}
