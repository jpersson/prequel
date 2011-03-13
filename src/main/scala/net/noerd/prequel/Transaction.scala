package net.noerd.prequel

import java.sql.Connection
import java.sql.Statement
import java.sql.ResultSet

import scala.collection.mutable.ArrayBuffer

import org.joda.time.DateTime

import net.noerd.prequel.RichConnection.conn2RichConn 

/**
 * A Transaction is normally created by the InTransaction object and can be
 * used to execute one or more queries against the database. Once the block
 * passed to InTransaction is succesfully executed the transaction is auto-
 * matically committed. And if some exception is throws during execution the 
 * transaction is rollbacked. 
 */
class Transaction( val connection: Connection ) {
    
    /**
     * Returns all records returned by the query after being converted by the
     * given block. 
     * 
     * @param sql query that should return records
     * @param params are the optional parameters used in the query
     * @param block is a function converting the row to something else
     */
    def select[ T ]( sql: String, params: Any* )( block: ( ResultSetRow ) => T ): Seq[ T ] = {
        _select( sql, params.toSeq )( block )
    }
    
    /**
     * Returns the first record returned by the query after being converted by the
     * given block. If the query does not return anything None is returned.
     * 
     * @param sql query that should return records
     * @param params are the optional parameters used in the query
     * @param block is a function converting the row to something else
     */
    def selectHead[ T ]( sql: String, params: Any* )( block: ( ResultSetRow ) => T ): Option[ T ] = {
        _select( sql, params.toSeq )( block ).headOption
    }
    
    /**
     * Returns a Long from a query that must guarantee to return at least 
     * one record which single column can be intepreted as a long
     *
     * @param sql query that must return at least one record with a long
     * @param params are the optional parameters used in the query
     */
    def selectLong( sql: String, params: Any* ): Long = {
        _select( sql, params.toSeq )( _.nextLong ).head
    }
    
    /**
     * Executes the given query and returns the number of affected records
     *
     * @param sql query that must not return any records
     * @param params are the optional parameters used in the query
     */
    def execute( sql: String, params: Any* ): Int = {
        connection.withStatement { statement =>
            statement.executeUpdate( SQL( sql, params.toSeq ) )
        }
    }
    
    /**
     * Rollbacks the Transaction. 
     *
     * @throws SQLException if transaction could not be rollbacked
     */
    def rollback(): Unit = connection.rollback()

    /**
     * Commits all changed done in the Transaction. 
     *
     * @throws SQLException if transaction could not be committed.
     */
    def commit(): Unit = connection.commit()
    
    private def _select[ T ]( sql: String, params: Seq[ Any ] )
    ( block: ( ResultSetRow ) => T ): Seq[ T ] = {
        connection.withStatement { statement =>
            val results: ArrayBuffer[ T ] = new ArrayBuffer
            val rs = statement.executeQuery( SQL( sql, params ) )
            
            while( rs.next ) {
                
                results.append( block( ResultSetRow( rs ) ) )
            }
            results
        }
    }
}

object Transaction {
    
    def apply( conn: Connection ) = new Transaction( conn )
}