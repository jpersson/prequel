package net.noerd.prequel

import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.Timestamp
import java.sql.Types

import org.joda.time.DateTime

/**
 * Wrapper around PreparedStatement making is easier to add parameters.
 *
 * The RichPreparedStatement can be used in two ways.
 *
 * ## Add parameters and then execute
 *     statement << param1 << param2 << param3 <<!
 *
 * ## Set parameters and execute in on shot
 *     statement.executeWith( param1, param2, param3 )
 *
 */
private class RichPreparedStatement( wrapped: PreparedStatement, formatter: SQLFormatter ) {
    private val StartIndex = 1
    private var parameterIndex = StartIndex
    
    /**
     * Adds the param to the query and returns this so that it
     * possible to chain several calls together
     */
    def <<( param: Formattable ): RichPreparedStatement = {
        param.addTo( this )
        this
    }
    
    /**
     * Add a String to the current parameter index
     */    
    def addString( value: String ) = addValue( () => 
        wrapped.setString( parameterIndex, formatter.escapeString( value ) ) 
    )
    
    /**
     * Add a Date to the current parameter index. This is done by setTimestamp which
     * looses the Timezone information of the DateTime
     */
    def addDateTime( value: DateTime ) = addValue( () => 
        wrapped.setTimestamp( parameterIndex, new Timestamp( value.getMillis ) ) 
    )
    /**
     * Add a Boolean to the current parameter index
     */    
    def addBoolean( value: Boolean ) = addValue( () => wrapped.setBoolean( parameterIndex, value ) )

    /**
     * Add a Long to the current parameter index
     */    
    def addLong( value: Long ) = addValue( () => wrapped.setLong( parameterIndex, value ) )    

    /**
     * Add a Int to the current parameter index
     */    
    def addInt( value: Int ) = addValue( () => wrapped.setInt( parameterIndex, value ) )

    /**
     * Add a Float to the current parameter index
     */    
    def addFloat( value: Float ) = addValue( () => wrapped.setFloat( parameterIndex, value ) )

    /**
     * Add a Double to the current parameter index
     */    
    def addDouble( value: Double ) = addValue( () => wrapped.setDouble( parameterIndex, value ) )    

    /**
     * Add Null to the current parameter index
     */    
    def addNull() = addValue( () => wrapped.setNull( parameterIndex, Types.NULL ) )

    /**
     * Sets all parameters and executes the statement 
     * @return the number of affected records
     */
    def executeWith( params: Formattable* ): Int = {
        params.foreach( this << _ )
        execute
    }
    
    def execute(): Int = {
        parameterIndex = StartIndex
        wrapped.executeUpdate()
    }
    
    def <<!(): Int = execute()
    
    def close() = wrapped.close()
    
    private def addValue( f: () => Unit ) = {
        f.apply
        parameterIndex = parameterIndex + 1
    }
}