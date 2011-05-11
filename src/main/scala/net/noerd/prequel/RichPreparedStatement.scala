package net.noerd.prequel

import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.Timestamp
import java.sql.Types

import org.joda.time.DateTime

private[prequel] class RichPreparedStatement( wrapped: PreparedStatement, formatter: SQLFormatter ) {
    private val StartIndex = 1
    private var parameterIndex = StartIndex
    
    def <<( param: Formattable ): RichPreparedStatement = {
        param.addTo( this )
        this
    }
    
    def set( params: Formattable* ): Unit = params.foreach( this << _ )
    
    def addString( value: String ) = addValue( () => 
        wrapped.setString( parameterIndex, formatter.escapeString( value ) ) 
    )
    def addDateTime( value: DateTime ) = addValue( () => 
        wrapped.setTimestamp( parameterIndex, new Timestamp( value.getMillis ) ) 
    )
    def addBoolean( value: Boolean ) = addValue( () => wrapped.setBoolean( parameterIndex, value ) )
    def addLong( value: Long ) = addValue( () => wrapped.setLong( parameterIndex, value ) )    
    def addInt( value: Int ) = addValue( () => wrapped.setInt( parameterIndex, value ) )
    def addFloat( value: Float ) = addValue( () => wrapped.setFloat( parameterIndex, value ) )
    def addDouble( value: Double ) = addValue( () => wrapped.setDouble( parameterIndex, value ) )    
    def addNull() = addValue( () => wrapped.setNull( parameterIndex, Types.NULL ) )
    
    def execute(): Int = {
        parameterIndex = StartIndex
        wrapped.executeUpdate()
    }
    def close() = wrapped.close()
    
    private def addValue( f: () => Unit ) = {
        f.apply
        parameterIndex = parameterIndex + 1
    }
}