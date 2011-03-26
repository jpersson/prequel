package net.noerd.prequel

/**
 * InTransaction is a factory object for Transaction instances. Given the
 * DatabaseConfig an existing pool is used or a new one is created that will
 * be reused for the same configuration the next time it's used. 
 */ 
object InTransaction { 
    /**
     * Given a block and a DatabaseConfig a Transaction will be created and 
     * passed to the block. If the block is executed succesfully the transaction 
     * will be committed but if an exception is throw it will be rollbacked 
     * immediately and rethrow the exception.
     *
     * @throws Any Exception that the block may generate.
     * @throws SQLException if the connection could not be committed, rollbacked
     *         or closed.
     */
    def apply[T]( block: ( Transaction ) => T )( implicit config: DatabaseConfig ): T = {
        val transaction = TransactionFactory.newTransaction( config )
        
        try {
            val value = block( transaction )
            transaction.commit()
            value
        }
        catch { 
            case th: Throwable => {
                transaction.rollback()
                throw th
            }
        }
        finally {
            transaction.connection.close()
        }
    }
}