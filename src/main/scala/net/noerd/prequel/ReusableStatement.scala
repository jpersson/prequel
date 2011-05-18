package net.noerd.prequel

import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.Timestamp
import java.sql.Types

import org.joda.time.DateTime

/**
 * Wrapper around PreparedStatement making is easier to add parameters.
 *
 * The ReusableStatement can be used in two ways.
 *
 * ## Add parameters and then execute as a chain
 *     statement << param1 << param2 << param3 <<!
 *
 * ## Set parameters and execute in on shot
 *     statement.executeWith( param1, param2, param3 )
 */
private class ReusableStatement( wrapped: PreparedStatement, formatter: SQLFormatter ) {
    private val StartIndex = 1
    private var parameterIndex = StartIndex
            
    /**
     * Adds the param to the query and returns this so that it
     * possible to chain several calls together
     * @return self to allow for chaining calls
     */
    def <<( param: Formattable ): ReusableStatement = {
        param.addTo( this )
        this
    }
    
    /**
     * Alias of execute() included to look good with the <<
     * @return the number of affected records
     */
    def <<!(): Int = execute()

    /**
     * Executes the statement with the previously set parameters
     * @return the number of affected records
     */
    def execute(): Int = {
        parameterIndex = StartIndex
        wrapped.executeUpdate()
    }

    /**
     * Sets all parameters and executes the statement 
     * @return the number of affected records
     */
    def executeWith( params: Formattable* ): Int = {
        params.foreach( this << _ )
        execute
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
    def addDateTime( value: DateTime ): Unit = addValue( () => 
        wrapped.setTimestamp( parameterIndex, new Timestamp( value.getMillis ) ) 
    )
    /**
     * Add a Boolean to the current parameter index
     */    
    def addBoolean( value: Boolean ): Unit = addValue( () => wrapped.setBoolean( parameterIndex, value ) )

    /**
     * Add a Long to the current parameter index
     */    
    def addLong( value: Long ): Unit = addValue( () => wrapped.setLong( parameterIndex, value ) )    

    /**
     * Add a Int to the current parameter index
     */    
    def addInt( value: Int ): Unit = addValue( () => wrapped.setInt( parameterIndex, value ) )

    /**
     * Add a Float to the current parameter index
     */    
    def addFloat( value: Float ): Unit = addValue( () => wrapped.setFloat( parameterIndex, value ) )

    /**
     * Add a Double to the current parameter index
     */    
    def addDouble( value: Double ): Unit = addValue( () => wrapped.setDouble( parameterIndex, value ) )    

    /**
     * Add Null to the current parameter index
     */    
    def addNull(): Unit = addValue( () => wrapped.setNull( parameterIndex, Types.NULL ) )
    
    private def addValue( f: () => Unit ) = {
        f.apply
        parameterIndex = parameterIndex + 1
    }

    private[prequel] def close() = wrapped.close()
}