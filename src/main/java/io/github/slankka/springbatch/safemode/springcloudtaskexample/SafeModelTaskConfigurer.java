package io.github.slankka.springbatch.safemode.springcloudtaskexample;

import org.springframework.cloud.task.configuration.DefaultTaskConfigurer;
import org.springframework.cloud.task.repository.TaskRepository;
import org.springframework.cloud.task.repository.support.SimpleTaskRepository;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;

@Component
public class SafeModelTaskConfigurer extends DefaultTaskConfigurer {

    private TaskRepository taskRepository;
    private DataSource dataSource;

    public SafeModelTaskConfigurer() {
        super();
    }

    public SafeModelTaskConfigurer(DataSource dataSource) {
        super(dataSource);
    }

    public SafeModelTaskConfigurer(String tablePrefix) {
        super(tablePrefix);
    }

    public SafeModelTaskConfigurer(DataSource dataSource, String tablePrefix,
                                   ApplicationContext context) {
        super(dataSource, tablePrefix, context);
        this.dataSource = dataSource;

        SafeTaskExecutionDaoFactoryBean safeTaskExecutionDaoFactoryBean;

        if (this.dataSource != null) {
            safeTaskExecutionDaoFactoryBean = new
                    SafeTaskExecutionDaoFactoryBean(this.dataSource, tablePrefix);
        } else {
            safeTaskExecutionDaoFactoryBean = new SafeTaskExecutionDaoFactoryBean();
        }

        this.taskRepository = new SimpleTaskRepository(safeTaskExecutionDaoFactoryBean);
    }

    @Override
    public TaskRepository getTaskRepository() {
        return this.taskRepository;
    }
}
