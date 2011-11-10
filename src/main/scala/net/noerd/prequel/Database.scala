package net.noerd.prequel

import java.sql.{ SQLException, Connection }
import java.util.Properties

import scala.collection.mutable.{ Map => MMap }
import scala.collection.mutable.HashMap

import org.apache.commons.dbcp.PoolableConnectionFactory
import org.apache.commons.dbcp.DriverManagerConnectionFactory
import org.apache.commons.dbcp.PoolingDataSource
import org.apache.commons.pool.KeyedObjectPoolFactory
import org.apache.commons.pool.impl.GenericObjectPool


private [prequel] object ConnectionPools {
    
    private val pools: MMap[ DatabaseConfig, PoolingDataSource ]= new HashMap
    private val UserProperty = "user"
    private val PasswordProperty = "password"    
    
    def nbrOfPools = pools.size
    
    def getOrCreatePool( config: DatabaseConfig ): PoolingDataSource = {
        pools.get( config ).getOrElse { 
            val connectionPool = new GenericObjectPool( 
                null, config.poolConfig.toGenericObjectPoolConfig 
            )
            val connectionProperties = mapAsProperties( Map( 
                UserProperty -> config.username, 
                PasswordProperty -> config.password 
            ) )
            val connectionFactory = new DriverManagerConnectionFactory( 
                config.jdbcURL, connectionProperties 
            )
            val defaultReadonly = false
            val defaultAutoCommit = false
            val stmtPoolFactory: KeyedObjectPoolFactory = null
            val validationQuery = "select 1"
            val poolableConnectionFactory = new PoolableConnectionFactory(
                connectionFactory, connectionPool, stmtPoolFactory, validationQuery, 
                defaultReadonly, defaultAutoCommit, config.isolationLevel.id
            )
            val dataSource: PoolingDataSource = {
                new PoolingDataSource( connectionPool )
            }
            
            pools.synchronized {
                pools += (( config, dataSource ))
            }
            
            dataSource
        }
    }
    
    // Conversion method to deal with the nasty java.util.Properties class
    private def mapAsProperties( aMap: Map[ String, String ] ): Properties = {
        val properties = new Properties
        aMap.map( pair => properties.setProperty( pair._1, pair._2 ) )
        properties
    }
    
    // Used during testing
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