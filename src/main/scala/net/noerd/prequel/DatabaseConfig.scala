package net.noerd.prequel

import java.sql.Connection.TRANSACTION_NONE
import java.sql.Connection.TRANSACTION_READ_COMMITTED
import java.sql.Connection.TRANSACTION_READ_UNCOMMITTED
import java.sql.Connection.TRANSACTION_REPEATABLE_READ
import java.sql.Connection.TRANSACTION_SERIALIZABLE

import org.apache.commons.pool.impl.GenericObjectPool.DEFAULT_MAX_ACTIVE
import org.apache.commons.pool.impl.GenericObjectPool.DEFAULT_MAX_IDLE
import org.apache.commons.pool.impl.GenericObjectPool.DEFAULT_MIN_IDLE
import org.apache.commons.pool.impl.GenericObjectPool.DEFAULT_MAX_WAIT
import org.apache.commons.pool.impl.GenericObjectPool.DEFAULT_MIN_EVICTABLE_IDLE_TIME_MILLIS
import org.apache.commons.pool.impl.GenericObjectPool.DEFAULT_TIME_BETWEEN_EVICTION_RUNS_MILLIS
import org.apache.commons.pool.impl.GenericObjectPool.DEFAULT_TEST_WHILE_IDLE
import org.apache.commons.pool.impl.GenericObjectPool

import org.joda.time.Duration

sealed abstract class TransactionIsolation( val id: Int )

object IsolationLevels {
    
    case object None extends TransactionIsolation( TRANSACTION_NONE )
    case object ReadCommitted extends TransactionIsolation( TRANSACTION_READ_COMMITTED )
    case object ReadUncommitted extends TransactionIsolation( TRANSACTION_READ_UNCOMMITTED )
    case object RepeatableRead extends TransactionIsolation( TRANSACTION_REPEATABLE_READ )
    case object Serializable extends TransactionIsolation( TRANSACTION_SERIALIZABLE )
}

/**
 * Configures how to connect to the database and how the connection 
 * should then be pooled.
 *
 * @param driver is a full class name of the JDBC Driver used.
 * @param jdbcURL is a qualified connection string with the db-type encoded (jdbc:psql...)
 * @param username to use when connecting to the db
 * @param password to use when connecting to the db
 * @param isolationLevel for transactions. (Default: ReadCommitted)
 * @param poolConfig configures how the connections should be pooled.
 */
final case class DatabaseConfig(
    val driver: String, 
    val jdbcURL: String, 
    val username: String = "", 
    val password: String = "",
    val isolationLevel: TransactionIsolation = IsolationLevels.ReadCommitted,
    val sqlFormatter: SQLFormatter = SQLFormatter.DefaultSQLFormatter,
    val poolConfig: PoolConfig = new PoolConfig
) {
    
    // Make sure that the class is available
    Class.forName( driver )
}

/**
 * Configures the details of the connection pooling. See GenericObjectPool.Config from
 * commons-pool for more details.
 *
 * This classes used the default values from GenericObjectPool if not specified.
 */
final case class PoolConfig(
    val maxActive: Int = DEFAULT_MAX_ACTIVE,
    val maxIdle: Int = DEFAULT_MAX_IDLE,
    val minIdle: Int = DEFAULT_MIN_IDLE,
    val maxWait: Duration = new Duration( DEFAULT_MAX_WAIT ),
    val evictionInterval: Duration = new Duration( DEFAULT_TIME_BETWEEN_EVICTION_RUNS_MILLIS ),
    val evictAfterIdleFor: Duration = new Duration( DEFAULT_MIN_EVICTABLE_IDLE_TIME_MILLIS ),
    val testWhileIdle: Boolean = DEFAULT_TEST_WHILE_IDLE    
) {
    
    def toGenericObjectPoolConfig: GenericObjectPool.Config = {
        
        val config = new GenericObjectPool.Config
        
        config.maxActive = maxActive
        config.maxIdle = maxIdle
        config.minIdle = minIdle
        config.maxWait = maxWait.getMillis
        config.timeBetweenEvictionRunsMillis = evictionInterval.getMillis
        config.testWhileIdle = testWhileIdle
        config.minEvictableIdleTimeMillis = evictAfterIdleFor.getMillis
    
        config
    }
}