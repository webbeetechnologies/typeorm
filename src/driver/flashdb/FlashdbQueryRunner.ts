import { QueryRunnerAlreadyReleasedError } from "../../error"
import { AbstractSqliteQueryRunner }       from "../sqlite-abstract/AbstractSqliteQueryRunner"
import { FlashdbDriver }                   from "./FlashdbDriver"
import { Broadcaster }                     from "../../subscriber/Broadcaster"
import { QueryFailedError }                from "../../error"
import { QueryResult }                     from "../../query-runner/QueryResult"

/**
 * Runs queries on a single sqlite database connection.
 */
export class FlashdbQueryRunner extends AbstractSqliteQueryRunner {

    /**
     * Database driver used by connection.
     */
    driver: FlashdbDriver

    // -------------------------------------------------------------------------
    // Constructor
    // -------------------------------------------------------------------------

    constructor(driver: FlashdbDriver) {
        super()
        this.driver = driver
        this.connection = driver.connection
        this.broadcaster = new Broadcaster(this)
    }

    // -------------------------------------------------------------------------
    // Public methods
    // -------------------------------------------------------------------------

    /**
     * Called before migrations are run.
     */
    async beforeMigration(): Promise<void> {
        await this.query(`PRAGMA foreign_keys = OFF`)
    }

    /**
     * Called after migrations are run.
     */
    async afterMigration(): Promise<void> {
        await this.query(`PRAGMA foreign_keys = ON`)
    }

    async release(): Promise<void> {
        return super.release()
    }

    /**
     * Commits transaction.
     * Error will be thrown if transaction was not started.
     */
    async commitTransaction(): Promise<void> {
        await super.commitTransaction()
    }

    /**
     * Executes a given SQL query.
     */
    async query(
        query: string,
        parameters: any[] = [],
        useStructuredResult = false,
    ): Promise<any> {
        if (this.isReleased) throw new QueryRunnerAlreadyReleasedError()

        const databaseConnection = this.driver.databaseConnection
        this.driver.connection.logger.logQuery(query, parameters, this)
        const queryStartTime = +new Date()

        try {
            // log slow queries if maxQueryExecution time is set
            const maxQueryExecutionTime =
                this.driver.options.maxQueryExecutionTime
            const queryEndTime = +new Date()
            const queryExecutionTime = queryEndTime - queryStartTime
            if (
                maxQueryExecutionTime &&
                queryExecutionTime > maxQueryExecutionTime
            )
                this.driver.connection.logger.logQuerySlow(
                    queryExecutionTime,
                    query,
                    parameters,
                    this,
                )

            const records: any[] = await databaseConnection.query(query, parameters);

            const result = new QueryResult()

            result.affected = await databaseConnection.getRowsModified()
            result.records = records
            result.raw = records

            if (useStructuredResult) {
                return result
            } else {
                return result.raw
            }
        } catch (e) {
            this.driver.connection.logger.logQueryError(
                e,
                query,
                parameters,
                this,
            )
            throw new QueryFailedError(query, parameters, e)
        }
    }
}
