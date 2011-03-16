package net.noerd.prequel

import java.util.Date

import java.sql.ResultSet
import java.sql.ResultSetMetaData

import scala.collection.mutable.ArrayBuffer

import org.joda.time.DateTime
import org.joda.time.Duration

/**
 * Provides access the the current row in the ResultSet. By calling any
 * of the next* methods it possible to step through the columns of the row.
 */
class ResultSetRow( private val rs: ResultSet ) {
      
    /** Maintain the current position. */
    private var position = 0
      
    private def incrementPosition = { position = position + 1 }
      
    /** 
     * Reset the column position. Setting it to 0 will cause
     * the next call of a next* method to return the first column
     *
     * @return self
     */
    def setColumnIndex( index: Int ): ResultSetRow = {
        position = index
        this
    }
  
    def nextBoolean: Boolean = {
        incrementPosition
        rs.getBoolean( position )
    }

    def nextByte: Byte = {
        incrementPosition        
        rs.getByte( position )
    }

    def nextInt: Int = {
        incrementPosition        
        rs.getInt( position )
    }
      
    def nextLong: Long = {
        incrementPosition
        rs.getLong( position )
    }
          
    def nextFloat: Float = {
        incrementPosition        
        rs.getFloat( position )
    }

    def nextDouble: Double = {
        incrementPosition        
        rs.getDouble( position )
    }

    def nextString: String = {
        incrementPosition
        rs.getString( position )
    }

    def nextDate: Date = {
        incrementPosition        
        rs.getTimestamp( position )
    }
      
    def nextDateTime: DateTime = {
        incrementPosition
        getDateTime( position )
    }        

    def nextDuration: Duration = {
        incrementPosition
        getDuration( position )
    }

    def nextObject: AnyRef = {
        incrementPosition
        rs.getObject( position )
    }
    
    def getBoolean( column: String ): Boolean = rs.getBoolean( column )
    def getByte( column: String ): Byte = rs.getByte( column )
    def getFloat( column: String ): Float = rs.getFloat( column )
    def getDouble( column: String ): Double = rs.getDouble( column )
    def getString( column: String ): String = rs.getString( column )
    def getLong( column: String ): Long = rs.getLong( column )
    def getInt( column: String ): Int = rs.getInt( column )
    def getDuration( column: String ): Int = rs.getInt( column )
    def getDate( column: String ): Date = rs.getTimestamp( column )
    def getDateTime( column: String ): DateTime = {
        getDateTime( rs.findColumn( column ) )
    }
        
    def columnNames: Seq[ String ]= {          
        val columnNames = ArrayBuffer.empty[ String ]
        val metaData = rs.getMetaData
        for(index <- 0.until( metaData.getColumnCount ) ) {
            columnNames += metaData.getColumnName( index + 1 ).toLowerCase
        }
        columnNames
    }
    
    private def getDateTime( index: Int ): DateTime = {
        val timestamp = rs.getTimestamp( index )
        if( timestamp != null )
            return new DateTime( timestamp.getTime )
        else 
            return null
    }

    private def getDuration( index: Int ): Duration = {
        val duration = rs.getLong( index )
        if( !rs.wasNull )
            return new Duration( duration )
        else 
            return null
    }
}

object ResultSetRow {
    
    def apply( rs: ResultSet ): ResultSetRow = {
        new ResultSetRow( rs )
    }
}
/**
 * Defines a number of implicit calls to the next* methods of the ResultSetRow.
 * 
 * Handles all types supported by the ResultSetRow as well as Options containing those.
 *
 * Example of implicit calls to the right next* method:
 *
 *     import net.noerd.prequel.ResultSetRowImplicits._
 *
 *     case class Person( id: Long, name: String, birthdate: DateTime )
 *
 *     InTransaction { tx =>
 *         tx.select( "select id, name, birthdate from people" ) { r =>
 *             Person( r, r, r )
 *         }
 *     }
 */
object ResultSetRowImplicits {
        
    implicit def row2Boolean( row: ResultSetRow ) = row.nextBoolean;
    implicit def row2Byte( row: ResultSetRow ) = row.nextByte;
    implicit def row2Int( row: ResultSetRow ) = row.nextInt;
    implicit def row2Long( row: ResultSetRow ) = row.nextLong;
    implicit def row2Float( row: ResultSetRow ) = row.nextFloat;
    implicit def row2Double( row: ResultSetRow ) = row.nextDouble;
    implicit def row2String( row: ResultSetRow ) = row.nextString;
    implicit def row2Date( row: ResultSetRow ) = row.nextDate;
    implicit def row2DateTime( row: ResultSetRow ) = row.nextDateTime
    implicit def row2Duration( row: ResultSetRow ) = row.nextDuration    
    implicit def row2BooleanOption( row: ResultSetRow ): Option[ Boolean ] = row2Option( row )
    implicit def row2ByteOption( row: ResultSetRow ): Option[ Byte ] = row2Option( row )
    implicit def row2IntOption( row: ResultSetRow ): Option[ Int ] = row2Option( row )
    implicit def row2LongOption( row: ResultSetRow ): Option[ Long ] = row2Option( row )
    implicit def row2FloatOption( row: ResultSetRow ): Option[ Float ] = row2Option( row )
    implicit def row2DoubleOption( row: ResultSetRow ): Option[ Double ] = row2Option( row )
    implicit def row2StringOption( row: ResultSetRow ): Option[ String ] = row2Option( row )
    implicit def row2DateOption( row: ResultSetRow ): Option[ java.util.Date ] = row2Option( row )
    // In order to support nullable DateTime objects we need a special implementation since we
    // can't cast the TimeStamp to a DateTime directly.
    implicit def row2DateTimeOption( row: ResultSetRow ): Option[ DateTime ] = {
    
        val ref = row.nextDateTime
        if ( ref == null ) None else Some( ref )
    }
    implicit def row2DurationOption( row: ResultSetRow ): Option[ Duration ] = {
    
        val ref = row.nextDuration
        if ( ref == null ) None else Some( ref )
    }
    
    private def row2Object( row: ResultSetRow ): AnyRef = row.nextObject
    
    private def row2Option[ T ]( row: ResultSetRow ): Option[ T ] = {
        val ref = row2Object( row )
        if ( ref == null ) None else Some[T]( ref.asInstanceOf[ T ] )
    }   
}