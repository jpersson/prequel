package net.noerd.prequel

import java.util.Date

import org.apache.commons.lang.StringEscapeUtils.escapeSql

import org.joda.time.DateTime
import org.joda.time.Duration
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatter
import org.joda.time.format.ISODateTimeFormat

/**
 * Currently a private class responsible for formatting SQL used in 
 * transactions (@see Transaction). It does properly format standards 
 * classes like DateTime, Floats, Longs and Integers as well as some 
 * SQL specific classes like Nullable, NullComparable and Identifier. 
 * See their documentation for more info on how to use them.
 */
class SQLFormatter(
    val timeStampFormatter: DateTimeFormatter
) {
    private val sqlQuote = "'"
    
    def format( sql: String, params: Formattable* ): String = formatSeq( sql, params.toSeq )

    def formatSeq( sql: String, params: Seq[ Formattable ] ): String = {
        sql.replace("?", "%s").format( params.map( p => p.escaped( this ) ): _* )
    }
    
    /**
     * Escapes  "'" and "\" in the string for use in a sql query
     */
    def escapeString( str: String ): String = escapeSql( str ).replace( "\\", "\\\\" )
    
    /**
     * Quotes the passed string according to the formatter 
     */
    def quoteString( str: String ): String = {
        val sb = new StringBuilder
        sb.append( sqlQuote ).append( str ).append( sqlQuote )
        sb.toString
    }
    
    /**
     * Escapes and quotes the given string
     */
    def toSQLString( str: String ): String = quoteString( escapeString( str ) )
}

object SQLFormatter {
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
        
    private[ prequel ] def apply( 
        timeStampFormatter: DateTimeFormatter = ISODateTimeFormat.dateTimeNoMillis 
    ) = {
        new SQLFormatter( timeStampFormatter )
    }
}

object SQLFormatterImplicits {
    implicit def string2Formattable( wrapped: String ) = StringFormattable( wrapped )
    implicit def boolean2Formattable( wrapped: Boolean ) = BooleanFormattable( wrapped )
    implicit def long2Formattable( wrapped: Long ) = LongFormattable( wrapped )
    implicit def int2Formattable( wrapped: Int ) = IntFormattable( wrapped )
    implicit def float2Formattable( wrapped: Float ) = FloatFormattable( wrapped )
    implicit def double2Formattable( wrapped: Double ) = DoubleFormattable( wrapped )
    implicit def dateTime2Formattable( wrapped: DateTime ) = DateTimeFormattable( wrapped )
    implicit def date2Formattable( wrapped: Date ) = DateTimeFormattable( wrapped )
    implicit def duration2Formattable( wrapped: Duration ) = new DurationFormattable( wrapped )
}