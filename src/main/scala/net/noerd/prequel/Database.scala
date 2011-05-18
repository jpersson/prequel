package net.noerd.prequel

import java.sql.{ SQLException, Connection }

import scala.collection.mutable.{ Map => MMap }
import scala.collection.mutable.HashMap

import org.apache.commons.dbcp.PoolableConnectionFactory
import org.apache.commons.dbcp.DriverManagerConnectionFactory
import org.apache.commons.dbcp.PoolingDataSource
import org.apache.commons.pool.KeyedObjectPoolFactory
import org.apache.commons.pool.impl.GenericObjectPool


private [prequel] object ConnectionPools {
    
    private val pools: MMap[ DatabaseConfig, PoolingDataSource ]= new HashMap
    
    def nbrOfPools = pools.size
    
    def getOrCreatePool( config: DatabaseConfig ): PoolingDataSource = {
        pools.get( config ).getOrElse { 
            val connectionPool = new GenericObjectPool( null, config.poolConfig.toGenericObjectPoolConfig )
            val connectionFactory = new DriverManagerConnectionFactory(
                config.jdbcURL, new java.util.Properties
            )
            val defaultReadonly = false
            val defaultAutoCommit = false
            val stmtPoolFactory: KeyedObjectPoolFactory = null
            val validationQuery = "select 1"
            val poolableConnectionFactory = new PoolableConnectionFactory(
                connectionFactory, connectionPool, stmtPoolFactory, validationQuery, 
                defaultReadonly, defaultAutoCommit, config.isolationLevel.id
            )
            val dataSource: PoolingDataSource = new PoolingDataSource( connectionPool )
            
            pools.synchronized {
                pools += (( config, dataSource ))
            }
            
            dataSource
        }
    }
    
    private[prequel] def reset(): Unit = pools.clear
}

private [prequel] object TransactionFactory {
    
    def newTransaction( config: DatabaseConfig ): Transaction = {
        Transaction(
            ConnectionPools.getOrCreatePool( config ).getConnection(),
            config.sqlFormatter
        )
    }
}