package net.noerd.prequel

import java.sql.Connection
import java.sql.Statement
import java.sql.ResultSet

import scala.collection.mutable.ArrayBuffer

import org.joda.time.DateTime
import org.joda.time.Duration

import net.noerd.prequel.RichConnection.conn2RichConn 
import net.noerd.prequel.ResultSetRowImplicits.row2Long
import net.noerd.prequel.ResultSetRowImplicits.row2Int
import net.noerd.prequel.ResultSetRowImplicits.row2Boolean
import net.noerd.prequel.ResultSetRowImplicits.row2String
import net.noerd.prequel.ResultSetRowImplicits.row2Float
import net.noerd.prequel.ResultSetRowImplicits.row2Double
import net.noerd.prequel.ResultSetRowImplicits.row2DateTime
import net.noerd.prequel.ResultSetRowImplicits.row2Duration

/**
 * A Transaction is normally created by the InTransaction object and can be
 * used to execute one or more queries against the database. Once the block
 * passed to InTransaction is succesfully executed the transaction is auto-
 * matically committed. And if some exception is throws during execution the 
 * transaction is rollbacked. 
 * 
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
    def select[ T ]( sql: String, params: Formattable* )( block: ResultSetRow => T ): Seq[ T ] = {
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
    def selectAndProcess( sql: String, params: Formattable* )( block: ResultSetRow => Unit ): Unit = {
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
    def selectHeadOption[ T ]( sql: String, params: Formattable* )( block: ResultSetRow => T ): Option[ T ] = {
        select( sql, params.toSeq: _* )( block ).headOption
    }

    /**
     * Return the head record from a query that must be guaranteed to return at least one record.
     * The query may return more records but those will be ignored.
     *
     * @param sql is a query that must return at least one record
     * @param params are the optional parameters of the query
     * @param block is a function converting the returned row to something useful.
     * @throws NoSuchElementException if the query did not return any records.
     */
    def selectHead[ T ]( sql: String, params: Formattable* )( block: ResultSetRow => T ): T = {
        select( sql, params.toSeq: _* )( block ).head
    }
    
    /** 
     * Convience method for intepreting the first column of the first record as a long
     * 
     * @param sql is a query that must return at least one record
     * @param params are the optional parameters of the query
     * @throws RuntimeException if the value is null
     * @throws SQLException if the value in the first column could not be intepreted as a long
     * @throws NoSuchElementException if the query did not return any records.
     */
    def selectLong( sql: String, params: Formattable* ): Long = {
        selectHead( sql, params.toSeq: _* )( row2Long )
    }

    /** 
     * Convience method for intepreting the first column of the first record as a Int
     * 
     * @param sql is a query that must return at least one record
     * @param params are the optional parameters of the query
     * @throws RuntimeException if the value is null
     * @throws SQLException if the value in the first column could not be intepreted as a Int
     * @throws NoSuchElementException if the query did not return any records.
     */
    def selectInt( sql: String, params: Formattable* ): Int = {
        selectHead( sql, params.toSeq: _* )( row2Int )
    }

    /** 
     * Convience method for intepreting the first column of the first record as a Boolean
     * 
     * @param sql is a query that must return at least one record
     * @param params are the optional parameters of the query
     * @throws RuntimeException if the value is null
     * @throws SQLException if the value in the first column could not be intepreted as a Boolean
     * @throws NoSuchElementException if the query did not return any records.
     */
    def selectBoolean( sql: String, params: Formattable* ): Boolean = {
        selectHead( sql, params.toSeq: _* )( row2Boolean )
    }
    
    /** 
     * Convience method for intepreting the first column of the first record as a String
     * 
     * @param sql is a query that must return at least one record
     * @param params are the optional parameters of the query
     * @throws RuntimeException if the value is null
     * @throws SQLException if the value in the first column could not be intepreted as a String
     * @throws NoSuchElementException if the query did not return any records.
     */
    def selectString( sql: String, params: Formattable* ): String = {
        selectHead( sql, params.toSeq: _* )( row2String )
    }

    /** 
     * Convience method for intepreting the first column of the first record as a Float
     * 
     * @param sql is a query that must return at least one record
     * @param params are the optional parameters of the query
     * @throws RuntimeException if the value is null
     * @throws SQLException if the value in the first column could not be intepreted as a Float
     * @throws NoSuchElementException if the query did not return any records.
     */
    def selectFloat( sql: String, params: Formattable* ): Float = {
        selectHead( sql, params.toSeq: _* )( row2Float )
    }

    /** 
     * Convience method for intepreting the first column of the first record as a Double
     * 
     * @param sql is a query that must return at least one record
     * @param params are the optional parameters of the query
     * @throws RuntimeException if the value is null
     * @throws SQLException if the value in the first column could not be intepreted as a Double
     * @throws NoSuchElementException if the query did not return any records.
     */
    def selectDouble( sql: String, params: Formattable* ): Double = {
        selectHead( sql, params.toSeq: _* )( row2Double )
    }

    /** 
     * Convience method for intepreting the first column of the first record as a DateTime
     * 
     * @param sql is a query that must return at least one record
     * @param params are the optional parameters of the query
     * @throws RuntimeException if the value is null
     * @throws SQLException if the value in the first column could not be intepreted as a DateTime
     * @throws NoSuchElementException if the query did not return any records.
     */
    def selectDateTime( sql: String, params: Formattable* ): DateTime = {
        selectHead( sql, params.toSeq: _* )( row2DateTime )
    }

    /** 
     * Convience method for intepreting the first column of the first record as a Duration
     * 
     * @param sql is a query that must return at least one record
     * @param params are the optional parameters of the query
     * @throws RuntimeException if the value is null
     * @throws SQLException if the value in the first column could not be intepreted as a Duration
     * @throws NoSuchElementException if the query did not return any records.
     */
    def selectDuration( sql: String, params: Formattable* ): Duration = {
        selectHead( sql, params.toSeq: _* )( row2Duration )
    }
    
    /**
     * Executes the given query and returns the number of affected records
     *
     * @param sql query that must not return any records
     * @param params are the optional parameters used in the query
     */
    def execute( sql: String, params: Formattable* ): Int = {
        connection.usingStatement { statement =>
            statement.executeUpdate( formatter.formatSeq( sql, params.toSeq ) )
        }
    }
    
    def batchExecute[ T ]( 
        sql: String, 
        items: Iterable[T] )
    ( block: (RichPreparedStatement, T) => Unit ): Int = {
        var affectedRecords = 0
        connection.usingPreparedStatement( sql, formatter ) { statement =>
            items.foreach { item =>
                block( statement, item )
                affectedRecords += statement.execute
            }
        }
        affectedRecords
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
        sql: String, params: Seq[ Formattable ]
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