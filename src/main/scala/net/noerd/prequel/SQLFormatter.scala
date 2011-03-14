package net.noerd.prequel

import java.util.Date

import org.apache.commons.lang.StringEscapeUtils.escapeSql

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatter
import org.joda.time.format.ISODateTimeFormat

/**
 * Wrap your optional value in NullComparable to compare with null if None. 
 *
 * Note: The '=' operator is added during formatting so don't include it in your SQL
 */    
case class NullComparable( wrapped: Option[ Any ] )

/**
 * Wrap your optional value in Nullable to have it converted to null if None
 */
case class Nullable( wrapped: Option[ Any ] )

/**
 * Wrap a parameter string in an Identifier to avoid escaping
 */ 
case class Identifier( wrapped: String )

/**
 * Currently a private object responsible for formatting SQL used in 
 * transactions (@see Transaction). It does properly format standards 
 * classes like DateTime, Floats, Longs and Integers as well as some 
 * SQL specific classes like Nullable, NullComparable and Identifier. 
 * See their documentation for more info on how to use them.
 */
private[ prequel ] class SQLFormatter(
    val timeStampFormatter: DateTimeFormatter
) {
    private val sqlQuote = "'"
    
    def format( sql: String, params: Seq[ Any ] ): String = {
        sql.format( params.map( escapeParam ): _* )
    }
                
    private def escapeString( str: String ) = {
        val sb = new StringBuilder
        
        sb.append( sqlQuote ).append(
            escapeSql( str ).replace( "\\", "\\\\" )
        ).append( sqlQuote )
        
        sb.toString
    }
    
    private def escapeDateTime(date: DateTime):Any = {
        escapeString( timeStampFormatter.print( date ) )
    }
        
    private def escapeParam( param: Any ): Any = param match {
        case str: String => escapeString( str )
        case dateTime: DateTime => escapeDateTime( dateTime )
        case date: Date => escapeDateTime( new DateTime( date.getTime ) )
        case int: Int => int.toString
        case long: Long => long.toString
        case double: Double => "%f".format( double )
        case float: Float => "%f".format( float )
        case Identifier( wrapped ) => wrapped
        case NullComparable( wrapped ) => {
            wrapped.map( "=" + escapeParam( _ ) ).getOrElse( "is null" )
        }
        case Nullable( wrapped ) => {
            wrapped.map( escapeParam( _ ) ).getOrElse( "null" )
        }
        case _ => param.toString
    }   
}

private[ prequel ] object SQLFormatter {
    
    /**
     * SQLFormatter for dbs supporting ISODateTimeFormat
     */
    val DefaultSQLFormatter = SQLFormatter()
    /**
     * SQLFormatter for usage with HSQLDB. 
     */
    val HSQLDBSQLFormatter = SQLFormatter(
        DateTimeFormat.forPattern( "yyyy-MM-dd HH:mm:ss.SSSS" )
    )
    
    def apply( timeStampFormatter: DateTimeFormatter = ISODateTimeFormat.dateTimeNoMillis ) = {
        new SQLFormatter( timeStampFormatter )
    }
}