package net.noerd.prequel

import java.util.Date

import org.apache.commons.lang.StringEscapeUtils.escapeSql

import org.joda.time.DateTime
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
private[ prequel ] object SQL {

    // Using ISODateTimeFormat from joda which is thread safe
    private val sqlTimestampFormat = ISODateTimeFormat.dateTimeNoMillis

    private val sqlQuote = "'"
    
    def apply( sql: String, params: Seq[ Any ] ): String = {
        sql.format( params.map( escapeSQL ):_* )
    }
                
    private def escapeSQLString( str: String ) = {
        val sb = new StringBuilder
        
        sb.append( sqlQuote ).append(
            escapeSql( str ).replace( "\\", "\\\\" )
        ).append( sqlQuote )
        
        sb.toString
    }
    
    private def escapeSQLDateTime(date: DateTime):Any =
        escapeSQLString( sqlTimestampFormat.print( date ) )
        
    private def escapeSQL( param: Any ): Any = param match {
        case str: String => escapeSQLString( str )
        case dateTime: DateTime => escapeSQLDateTime( dateTime )
        case date: Date => escapeSQLDateTime( new DateTime( date.getTime ) )
        case int: Int => int.toString
        case long: Long => long.toString
        case double: Double => "%f".format( double )
        case float: Float => "%f".format( float )
        case Identifier( wrapped ) => wrapped
        case NullComparable( wrapped ) => {
            wrapped.map( "=" + escapeSQL( _ ) ).getOrElse( "is null" )
        }
        case Nullable( wrapped ) => {
            wrapped.map( escapeSQL( _ ) ).getOrElse( "null" )
        }
        case _ => param
    }   
}