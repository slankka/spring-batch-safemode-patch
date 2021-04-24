package io.github.slankka.springbatch.safemode.patch;

import org.springframework.batch.item.database.support.DefaultDataFieldMaxValueIncrementerFactory;
import org.springframework.batch.support.DatabaseType;
import org.springframework.jdbc.support.incrementer.DataFieldMaxValueIncrementer;

import javax.sql.DataSource;

/**
 * project: springbatch safemode patch
 * <br/>To prevent error: <br/>
 * <code>
 * Could not increment ID for BATCH_JOB_SEQ sequence table; <br/>
 * nested exception is java.sql.SQLException: <br/>
 * You are using safe update mode and you tried to update a table without a WHERE that uses a KEY column <br/>
 * </code>
 *
 * @author slankka on 2019/8/30.
 */
public class SafeModeMysqlIncrementFactory extends DefaultDataFieldMaxValueIncrementerFactory {

    private DataSource dataSource;
    private String incrementerColumnName = "ID";

    public SafeModeMysqlIncrementFactory(DataSource dataSource) {
        super(dataSource);
        this.dataSource = dataSource;
    }

    @Override
    public void setIncrementerColumnName(String incrementerColumnName) {
        super.setIncrementerColumnName(incrementerColumnName);
        this.incrementerColumnName = incrementerColumnName;
    }

    @Override
    public DataFieldMaxValueIncrementer getIncrementer(String incrementerType, String incrementerName) {
        DatabaseType databaseType = DatabaseType.valueOf(incrementerType.toUpperCase());
        if (databaseType == DatabaseType.MYSQL) {
            SafeModeMysqlMaxValueIncrementer mySQLMaxValueIncrementer = new SafeModeMysqlMaxValueIncrementer(dataSource, incrementerName, incrementerColumnName);
            mySQLMaxValueIncrementer.setUseNewConnection(true);
            return mySQLMaxValueIncrementer;
        }
        return super.getIncrementer(incrementerType, incrementerName);
    }
}
