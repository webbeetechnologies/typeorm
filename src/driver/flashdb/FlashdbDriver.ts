import { AbstractSqliteDriver }           from "../sqlite-abstract/AbstractSqliteDriver"
import { FlashdbConnectionOptions }       from "./FlashdbConnectionOptions"
import { FlashdbQueryRunner }             from "./FlashdbQueryRunner"
import { QueryRunner }                    from "../../query-runner/QueryRunner"
import { DataSource }                     from "../../data-source"
import { DriverPackageNotInstalledError } from "../../error"
import { PlatformTools }                  from "../../platform/PlatformTools"
import { ReplicationMode }                from "../types/ReplicationMode"

// This is needed to satisfy the typescript compiler.
interface Window {
    SQL: any
    localforage: any
}

declare let window: Window

export class FlashdbDriver extends AbstractSqliteDriver {
    // The driver specific options.
    options: FlashdbConnectionOptions

    // -------------------------------------------------------------------------
    // Constructor
    // -------------------------------------------------------------------------

    constructor(connection: DataSource) {
        super (connection)

        // load sql.js package
        this.loadDependencies ()
    }

    // -------------------------------------------------------------------------
    // Public Methods
    // -------------------------------------------------------------------------

    /**
     * Performs connection to the database.
     */
    async connect(): Promise<void> {
        this.databaseConnection = await this.createDatabaseConnection ().connect()
    }

    /**
     * Closes connection with database.
     */
    async disconnect(): Promise<void> {
        this.queryRunner = undefined
        this.databaseConnection.close ()
    }

    /**
     * Creates a query runner used to execute database queries.
     */
    createQueryRunner(mode: ReplicationMode): QueryRunner {
        if (!this.queryRunner) this.queryRunner = new FlashdbQueryRunner (this)

        return this.queryRunner
    }

    // -------------------------------------------------------------------------
    // Protected Methods
    // -------------------------------------------------------------------------

    /**
     * Creates connection with the database.
     * If the location option is set, the database is loaded first.
     */
    protected createDatabaseConnection(): any {
        return new this.sqlite.Database ();
    }

    /**
     * If driver dependency is not given explicitly, then try to load it via "require".
     */
    protected loadDependencies(): void {
        if (PlatformTools.type === "browser") {
            this.sqlite = this.options.driver || window.SQL
        } else {
            try {
                this.sqlite = this.options.driver || PlatformTools.load ("sql.js")
            } catch (e) {
                throw new DriverPackageNotInstalledError ("sql.js", "sql.js")
            }
        }
    }
}
