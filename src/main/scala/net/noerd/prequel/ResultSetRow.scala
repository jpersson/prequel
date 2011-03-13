package net.noerd.prequel

import java.util.Date

import java.sql.ResultSet
import java.sql.ResultSetMetaData

import scala.collection.mutable.{ Set => MSet }

import org.joda.time.DateTime

class ResultSetRow( private val rs: ResultSet ) {
      
    /** Maintain the current position. */
    private var position = 1
      
    private def incrementPosition { position = position + 1 }
      
    /** 
     * Reset the column position. The index starts with 1.
     */
    def apply( index: Int ): ResultSetRow = {
        position = index
        this
    }
  
    def nextBoolean: Boolean = {
        val bool = rs.getBoolean( position )
        incrementPosition
        return bool
    }

    def nextByte: Byte = {
        val byte = rs.getByte( position )
        incrementPosition
        return byte
    }

    def nextInt: Int = {
        val int = rs.getInt( position )
        incrementPosition
        return int
    }
      
    def nextLong: Long = {
        val long = rs.getLong( position )
        incrementPosition
        return long
    }
          
    def nextFloat: Float = {
        val float = rs.getFloat( position )
        incrementPosition
        return float
    }

    def nextDouble: Double = {
        val double = rs.getDouble( position )
        incrementPosition
        return double
    }

    def nextString: String = {
        val string = rs.getString( position )
        incrementPosition
        return string
    }

    def nextDate: Date = {
        val timestamp = rs.getTimestamp( position )
        incrementPosition
        return timestamp
    }
      
    def nextDateTime: DateTime = {
        val timestamp = rs.getTimestamp( position )
        incrementPosition
        if( timestamp != null )
            return new DateTime( timestamp.getTime )
        else 
            return null
    }        

    def nextObject: AnyRef = {
        val obj = rs.getObject( position )
        incrementPosition
        return obj
    }

    def columns = {          
        val columnNames = MSet.empty[ String ]
        val metaData = rs.getMetaData
        for(index <- 0.until( metaData.getColumnCount ) ) {
            columnNames + metaData.getColumnName( index + 1 )
        }
        columnNames
    }        
}

object ResultSetRow {
    
    def apply( rs: ResultSet ): ResultSetRow = {
        new ResultSetRow( rs )
    }
        
    implicit def row2Boolean( row: ResultSetRow ) = row.nextBoolean;
    implicit def row2Byte( row: ResultSetRow ) = row.nextByte;
    implicit def row2Int( row: ResultSetRow ) = row.nextInt;
    implicit def row2Long( row: ResultSetRow ) = row.nextLong;
    implicit def row2Float( row: ResultSetRow ) = row.nextFloat;
    implicit def row2Double( row: ResultSetRow ) = row.nextDouble;
    implicit def row2String( row: ResultSetRow ) = row.nextString;
    implicit def row2Date( row: ResultSetRow ) = row.nextDate;
    implicit def row2DateTime( row: ResultSetRow ) = row.nextDateTime
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
    
    private def row2Object( row: ResultSetRow ): AnyRef = row.nextObject
    
    private def row2Option[ T ]( row: ResultSetRow ): Option[ T ] = {
        val ref = row2Object( row )
        if ( ref == null ) None else Some[T]( ref.asInstanceOf[ T ] )
    }   
}