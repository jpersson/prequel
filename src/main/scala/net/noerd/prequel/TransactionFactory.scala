package net.noerd.prequel

object TransactionFactory {
    
    def newTransaction( config: DatabaseConfig ): Transaction = {
        Transaction(
            ConnectionPools.getOrCreatePool( config ).getConnection(),
            config.sqlFormatter
        )
    }
}
