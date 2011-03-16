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
 * 
 * ----------
 * @throws SQLException all methods executing queries will throw SQLException 
 *         if the query was not properly formatted or something went wrong in 
 *         the database during execution.
 *
 * @throws IllegalFormatException: Will be throw by all method if the format 
 *         string is invalid or if there is not enough parameters.
 */
class Transaction( val connection: Connection, val formatter: SQLFormatter ) {
    
    /**
     * Returns all records returned by the query after being converted by the
     * given block. All objects are kept in memory to this method is no suited
     * for very big result sets. Use selectAndProcess if you need to process 
     * bigger datasets.
     * 
     * @param sql query that should return records
     * @param params are the optional parameters used in the query
     * @param block is a function converting the row to something else
     */
    def select[ T ]( sql: String, params: Any* )( block: ResultSetRow => T ): Seq[ T ] = {
        val results = new ArrayBuffer[ T ]
        _selectIntoBuffer( Some( results ), sql, params.toSeq )( block )
        results
    }

    /**
     * Executes the query and passes each row to the given block. This method
     * does not keep the objects in memory and returns Unit so the row needs to
     * be fully processed in the block.
     * 
     * @param sql query that should return records
     * @param params are the optional parameters used in the query
     * @param block is a function fully processing each row
     */
    def selectAndProcess( sql: String, params: Any* )( block: ResultSetRow => Unit ): Unit = {
        _selectIntoBuffer( None, sql, params.toSeq )( block )
    }

    
    /**
     * Returns the first record returned by the query after being converted by the
     * given block. If the query does not return anything None is returned.
     * 
     * @param sql query that should return records
     * @param params are the optional parameters used in the query
     * @param block is a function converting the row to something else
     */
    def selectHead[ T ]( sql: String, params: Any* )( block: ResultSetRow => T ): Option[ T ] = {
        select( sql, params.toSeq: _* )( block ).headOption
    }
    
    /**
     * Returns a Long from a query that must guarantee to return at least 
     * one record which single column can be intepreted as a long
     *
     * @param sql query that must return at least one record with a long
     * @param params are the optional parameters used in the query
     * @throws SQLException if returned value is not a Long
     * @throws NoSuchElementException if no record was returned
     */
    def selectLong( sql: String, params: Any* ): Long = {
        select( sql, params.toSeq: _* )( _.nextLong ).head
    }
    
    /**
     * Executes the given query and returns the number of affected records
     *
     * @param sql query that must not return any records
     * @param params are the optional parameters used in the query
     */
    def execute( sql: String, params: Any* ): Int = {
        connection.usingStatement { statement =>
            statement.executeUpdate( formatter.formatSeq( sql, params.toSeq ) )
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
    
    private def _selectIntoBuffer[ T ]( 
        buffer: Option[ ArrayBuffer[T] ], 
        sql: String, params: Seq[ Any ]
    )( block: ( ResultSetRow ) => T ): Unit = {
        connection.usingStatement { statement =>
            val rs = statement.executeQuery( formatter.formatSeq( sql, params ) )
            val append = buffer.isDefined
            
            while( rs.next ) {    
                val value = block( ResultSetRow( rs ) )
                if( append ) buffer.get.append( value )
            }
        }
    }
}

object Transaction {
    
    def apply( conn: Connection, formatter: SQLFormatter ) = new Transaction( conn, formatter )
}