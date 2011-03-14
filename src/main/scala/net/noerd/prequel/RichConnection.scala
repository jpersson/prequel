package net.noerd.prequel

import java.sql.Connection
import java.sql.Statement

private[prequel] class RichConnection( val wrapped: Connection ) {
    
    def usingStatement[ T ]( block: (Statement) => T ): T = {
        val statement = wrapped.createStatement
        
        try {
            block( statement )
        }
        finally {
            // This also closes the resultset
            statement.close()
        }       
    }
}

private[prequel] object RichConnection {
    
    implicit def conn2RichConn( conn: Connection ): RichConnection = {
        new RichConnection( conn )
    }
}
