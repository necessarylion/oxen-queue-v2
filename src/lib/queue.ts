import * as crypto from 'crypto';
import { isUndefined, isNumber } from 'lodash';
import mysql from 'mysql2/promise';

import * as storage from './storage';
import {
  QueueConfig,
  ProcessConfig,
  DatabaseJob,
  QueueDebug,
  RetryConfig,
  SuccessConfig,
  ErrorConfig,
  Job,
  JobResult,
  JobError
} from '../types';

const errorMessages = {
  noJobType:
    'Must specify the job type e.g. const ox = new oxen_queue({ jobType: "weekly_emails" ... })',
  alreadyProcessing: `This queue is already processing`,
  workFnMissing: `Missing workFn argument, nothing to do! Remember that the process() function takes an object as its single argument.`,
  invalidConcurrency: `The concurrency argument must be a number`,
  noMysqlConnection: `Must supply mysqlConfig argument. It should look something like: const queue = new Queue({ mysqlConfig: { host : 'foo.net', password : 'secret'} ... }).`,
};

export class Queue<T = any> {
  private jobType: string;
  private extraFields: string[];
  private processing: boolean;
  private currentlyFetching: boolean;
  private inProcess: number;
  private workingJobBatch: DatabaseJob[];
  private jobRecoveryInterval: NodeJS.Timeout | null;
  private onJobError: (error: JobError) => Promise<void> | void;
  private onJobSuccess: (result: JobResult) => Promise<void> | void;
  private db: mysql.Pool;
  private dbTable: string;
  private pollingRate: number;
  private fastestPollingRate: number;
  private slowestPollingRate: number;
  private pollingBackoffRate: number;
  private batchSize?: number;
  private jobTimeoutSeconds?: number;
  private maxRetry?: number;
  private retryDelay?: number | ((attempt: number) => number);

  constructor(
    jobType: string,
    {
      mysqlConfig,
      dbTable = 'oxen_queue',
      extraFields = [],
      fastestPollingRate = 100,
      slowestPollingRate = 10000,
      pollingBackoffRate = 1.1,
      onJobSuccess = async () => { },
      onJobError = async () => { },
    }: QueueConfig) {
    if (!mysqlConfig) {
      throw new Error(errorMessages['noMysqlConnection']);
    }

    if (!jobType) {
      throw new Error(errorMessages['noJobType']);
    }

    /* Queue */
    this.jobType = jobType;
    this.extraFields = extraFields;
    this.processing = false;
    this.currentlyFetching = false;
    this.inProcess = 0;
    this.workingJobBatch = [];
    this.jobRecoveryInterval = null;

    /* Callbacks */
    this.onJobError = onJobError;
    this.onJobSuccess = onJobSuccess;

    /* Database */
    this.db = mysql.createPool(mysqlConfig);
    this.dbTable = dbTable;

    /* Polling */
    this.pollingRate = fastestPollingRate;
    this.fastestPollingRate = fastestPollingRate;
    this.slowestPollingRate = slowestPollingRate;
    this.pollingBackoffRate = pollingBackoffRate;
  }

  async addJob(job: Job<T>): Promise<void> {
    await this.addJobs([job]);
  }

  async addJobs(jobs: (Job<T>)[]): Promise<void> {
    if (jobs.length === 0) {
      return;
    }

    const fields = [
      'body',
      'job_type',
      'unique_key',
      'priority',
      'created_ts',
      ...this.extraFields,
    ];

    await this.dbQry(
      `
        INSERT INTO ${this.dbTable} (
          ${fields.join(',')}
        ) 
        VALUES ? 
        ON DUPLICATE KEY UPDATE 
        priority = IF(priority > VALUES(priority), VALUES(priority), priority)
      `,
      [
        jobs.map((job: Job<T> | any) => {
          if (isUndefined(job.body)) {
            job = { body: job };
          }

          const uniqueKey = isUndefined(job.uniqueKey)
            ? null
            : parseInt(
              crypto
                .createHash('md5')
                .update(`${job.uniqueKey}|${this.jobType}`)
                .digest('hex')
                .slice(0, 8),
              16
            );

          const createdTs = job.startTime ? new Date(job.startTime) : new Date();

          const inserts = [
            JSON.stringify(job.body),
            this.jobType,
            uniqueKey,
            isNumber(job.priority) ? job.priority : Date.now(),
            createdTs,
          ];

          this.extraFields.forEach(field => {
            inserts.push(job.body[field]);
          });

          return inserts;
        }),
      ]
    );
  }

  async process({
    workFn,
    concurrency = 10,
    timeout = 60, // 60 seconds
    recoverStuckJobs = true,
    maxRetry = 0,
    retryDelay = 0,
  }: ProcessConfig<T>): Promise<void> {
    if (this.processing) {
      throw new Error(errorMessages['alreadyProcessing']);
    }
    if (!workFn) {
      throw new Error(errorMessages['workFnMissing']);
    }

    if (!isNumber(concurrency)) {
      throw new Error(errorMessages['invalidConcurrency']);
    }

    this.batchSize = concurrency;
    this.jobTimeoutSeconds = Math.floor(timeout);
    this.maxRetry = maxRetry;
    this.retryDelay = retryDelay;

    this.processing = true;

    const loop = async (): Promise<void> => {
      if (this.inProcess < concurrency) {
        this.inProcess++;

        this.doWork(workFn)
          .then(() => {
            this.inProcess--;
          })
          .catch(error => {
            this.inProcess--;
            this.log('Unhandled Oxen Queue error:');
            this.log(error);
          });
      } else if (this.pollingRate < this.slowestPollingRate) {
        this.pollingRate *= this.pollingBackoffRate;
      }

      if (!this.processing) {
        return;
      }

      setTimeout(() => {
        if (!this.processing) {
          return;
        }
        loop();
      }, this.pollingRate);
    };

    loop();

    this.jobRecoveryInterval = setInterval(() => {
      if (recoverStuckJobs) {
        this.recoverStuckJobs().catch(error => {
          this.log('Unable to recover stuck jobs:');
          this.log(error);
        });
      } else {
        this.markStuckJobs().catch(error => {
          this.log('Unable to mark stuck jobs:');
          this.log(error);
        });
      }
    }, 1000 * 60); // every minute
  }

  stopProcessing(): void {
    this.processing = false;
    if (this.jobRecoveryInterval) {
      clearInterval(this.jobRecoveryInterval);
    }
  }

  private async doWork(workFn: (jobBody: T, job: DatabaseJob) => Promise<any> | any): Promise<string | void> {
    const job = await this.getNextJob();

    if (!job) {
      if (this.pollingRate < this.slowestPollingRate) {
        this.pollingRate *= this.pollingBackoffRate;
      } else if (this.pollingRate >= this.slowestPollingRate) {
        this.pollingRate = this.slowestPollingRate;
      }

      return Promise.resolve('no_job');
    }

    this.pollingRate = this.fastestPollingRate;

    return Promise.resolve(workFn(job.body, job))
      .then(async (jobResult: any) => {
        return this.handleSuccess({ jobId: job.id, jobResult, jobBody: job.body });
      })
      .catch(async (error: any) => {
        const countTry = await this.dbQry(
          `SELECT recovered FROM ${this.dbTable} WHERE ?`,
          { id: job.id }
        );
        if (countTry[0].recovered < this.maxRetry!) {
          return this.handleRetry({
            jobId: job.id,
            retrySeconds: this.retryDelay!,
            currentTry: (countTry[0].recovered ?? 0) + 1
          });
        }
        return this.handleError({ jobId: job.id, error, jobBody: job.body });
      });
  }

  private async getNextJob(): Promise<DatabaseJob | null> {
    if (!this.currentlyFetching && this.workingJobBatch.length < this.batchSize!) {
      this.currentlyFetching = true;

      await this.fillJobBatch().catch(error => {
        this.log('There was an error while trying to get the next set of jobs:');
        this.log(error);
      });
      this.currentlyFetching = false;
    }
    return this.workingJobBatch.shift() || null;
  }

  private async fillJobBatch(): Promise<boolean> {
    const batchId = Math.floor(Math.random() * Number.MAX_SAFE_INTEGER);

    const lockedBatch = await this.dbQry(
      `
      UPDATE ${this.dbTable} AS main
      INNER JOIN (
        SELECT id FROM ${this.dbTable} FORCE INDEX (locking_update)
        WHERE batch_id IS NULL 
        AND STATUS = "waiting" 
        AND ? 
        AND created_ts <= ${nowSql()}
        ORDER BY priority ASC LIMIT ${this.batchSize}
      ) sub
      ON sub.id = main.id
      SET ?, STATUS = "processing", started_ts = ${nowSql()}`,
      [{ job_type: this.jobType }, { batch_id: batchId }]
    );

    if (lockedBatch.changedRows === 0) {
      return false;
    }

    const nextJobs = await this.dbQry(
      `SELECT id, body FROM ${this.dbTable} WHERE ? ORDER BY priority ASC LIMIT ${this.batchSize}`,
      { batch_id: batchId }
    );

    if (nextJobs.length === 0) {
      return false;
    }

    nextJobs.forEach((job: any) => {
      job.body = JSON.parse(job.body);
      this.workingJobBatch.push(job);
    });

    return true;
  }

  private async handleSuccess({ jobId, jobResult, jobBody }: SuccessConfig): Promise<any> {
    await this.onJobSuccess({ jobId, jobBody, jobType: this.jobType, jobResult });
    return this.dbQry(
      `
      UPDATE ${this.dbTable} 
      SET 
        ?, 
        unique_key = NULL,
        status="success",
        running_time = TIMESTAMPDIFF(SECOND,started_ts,${nowSql()})  
      WHERE ? 
      LIMIT 1`,
      [
        {
          result: JSON.stringify(jobResult),
        },
        {
          id: jobId,
        },
      ]
    );
  }

  private async handleRetry({ jobId, retrySeconds, currentTry }: RetryConfig): Promise<any> {
    let retryIn = 0;

    if (typeof retrySeconds === 'number') {
      retryIn = retrySeconds;
    } else if (typeof retrySeconds === 'function') {
      retryIn = retrySeconds(currentTry);
    }

    return this.dbQry(
      `
      UPDATE ${this.dbTable} 
      SET 
         status = "waiting", 
         batch_id = NULL, 
         started_ts = NULL,
         created_ts = DATE_ADD(created_ts, INTERVAL ? SECOND),
         recovered = recovered + 1
      WHERE ? 
      LIMIT 1`,
      [retryIn, { id: jobId }]
    );
  }

  private async handleError({ jobId, error, jobBody }: ErrorConfig): Promise<any> {
    await this.onJobError({ jobId, jobBody, jobType: this.jobType, error });
    return this.dbQry(
      `
      UPDATE ${this.dbTable} 
      SET 
        ?, 
        unique_key = NULL,
        status="error", 
        running_time = TIMESTAMPDIFF(SECOND,started_ts,${nowSql()}) 
      WHERE ? 
      LIMIT 1`,
      [
        {
          result: error.stack,
        },
        {
          id: jobId,
        },
      ]
    );
  }

  async recoverStuckJobs(): Promise<any> {
    // reset jobs that are status=processing and have been created over 2 mins ago
    return this.dbQry(
      `
      UPDATE ${this.dbTable} 
      SET 
        STATUS="waiting", 
        batch_id = NULL, 
        started_ts = NULL, 
        recovered = 1 
      WHERE 
        STATUS="processing" AND 
        started_ts < (${nowSql()} - INTERVAL ${this.jobTimeoutSeconds} SECOND) AND
        ?
      `,
      { job_type: this.jobType }
    );
  }

  async markStuckJobs(): Promise<any> {
    // mark jobs that are status=processing and have been created over 2 mins ago
    return this.dbQry(
      `
      UPDATE ${this.dbTable} 
      SET 
        STATUS="stuck", 
        unique_key = NULL, 
        recovered = 1 
      WHERE 
        STATUS="processing" AND 
        started_ts < (${nowSql()} - INTERVAL ${this.jobTimeoutSeconds} SECOND) AND
        ?
      `,
      { job_type: this.jobType }
    );
  }

  private async dbQry(query: string, params?: any): Promise<any> {
    const errorsToRetry = [
      'ER_LOCK_WAIT_TIMEOUT',
      'ER_LOCK_DEADLOCK',
      'ETIMEDOUT',
      'ECONNREFUSED',
      'try restarting transaction',
    ];

    let retries = 5;

    while (retries > 0) {
      try {
        const [result] = await this.db.query(query, params);
        return result;
      } catch (e: any) {
        retries--;
        if (retries === 0 || !errorsToRetry.some(msg => e.message.includes(msg))) {
          throw e;
        }
      }

      await new Promise(resolve => setTimeout(resolve, Math.random() * 500 + 500));
    }
  }

  async createTable(): Promise<any> {
    const query = storage.createTable.replace('[[TABLE_NAME]]', this.dbTable);
    return this.dbQry(query);
  }

  async deleteTable(): Promise<any> {
    const query = storage.deleteTable.replace('[[TABLE_NAME]]', this.dbTable);
    return this.dbQry(query);
  }

  async selectEntireTable(): Promise<any> {
    const query = storage.selectEntireTable.replace('[[TABLE_NAME]]', this.dbTable);
    return this.dbQry(query);
  }

  debug(): QueueDebug {
    return {
      processing: this.processing,
      inProcess: this.inProcess,
      currentlyFetching: this.currentlyFetching,
      workingJobBatch: this.workingJobBatch,
    };
  }

  private log(msg: any, notError?: boolean): void {
    console.log(msg)
    console[notError ? 'log' : 'error'](`Oxen Queue: ${msg}`);
  }
}

function nowSql(): string {
  const timezone = process.env.TZ ?? 'UTC';
  return `CONVERT_TZ(NOW(), 'UTC', '${timezone}')`
}

export { errorMessages };
