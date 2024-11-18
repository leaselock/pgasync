"Async" is a postgres extension which allows placing queries on a processing 
queue for deferrred execution.  This is useful for pushing work into the 
background in a way that can be easily threaded.  This is useful for batch 
processing as well as many other cases that come up in data processing.

Async is an all SQL library, and has no required dependencies other than dblink 
which comes pacakged with the database.  This makes deployment very simple and 
portable across many runtime environments such as cloud computing.

Async defines the following concepts:
  * server: a database that contains configuration data and housekeeping 
    information around runninng tasks.  This can be a standalone database
    or installed to coexist within an existing database
  * async.main(): a stored procedure which starts up and never terminates
    (excepting unusual errors and other edge cases. 
  * target: a postgres database that runs tasks
  * task: a query batched for execution
  * concurrency pool: a limit on concurrent tasks organized under a name
  * asynchronous tasks:  tasks that call back to report task completion rather
    resolve completion with the invoking query.

Installing the async library:

Starting the orchestrator process:
  The ochestrator process is called via: CALL async.main(<force>).   This 
  process will start and never terminate unless cancelled or an error occurs.
  If async.main() returns immediately, it means that the orchestrator process
  is already runnning. 
  If async.main() is passed with the force flag set true, any orchestration
  already running will be terminated, and the terminating process will wait
  and take over so that *it* can become th orchestratior.


Updating the async library: TBD

Configuring the library:
  Async is configured in JSONB via async.configure().  The configuration 
  has the following structure:
  {
    "control": <control>,
    "targets": [<target>, ...],
    "concurrency_pools": [<pool>, ]
  }

The "Contol" object is optional.  Individual elements of the object are also 
optional as all options are defaulted.  The following values can be configured:

  * workers INT DEFAULT 20:  how many tasks can be held open concurrently 
    across all targets.  Asynchonous tasks do not count towards this limit while
    waiting for asynchronous response.  This essentially caps the number of 
    connections dblink is allowed to open.

  * idle_sleep FLOAT8 DEFAULT 1.0: when not busy, async.main() will sleep for 
    this amount of time before checkign again.

  * heavy_maintenance_sleep INTERVAL DEFAULT '24 hours'
    certain heavy tasks such as scrubbing old tasks data are once per this 
    interval of time.

  * task_keep_duration INTERVAL DEFAULT '30 days': controls how long task data
    is kept for analysis before deletion.

  * default_timeout INTERVAL DEFAULT: tasks will be cancelled (if possible) 
    and presumed failed if running for longer than a certain period of time. 
    This value controls the default.

  * advisory_mutex_id INT DEFAULT 0: async.main() takes out an advisory lock to 
    ensure only oene orchestrator process is running.  This value can be 
    adjuted to avoid advisory lock confict with other running queries.
    
  * self_target TEXT DEFAULT 'async.self': async pushes operations to itself 
    to be orchestrated.  This can be adjusted to avoid name conflicts with 
    other targets if needed.

  * self_connection_string TEXT DEFAULT 'host=localhost': The connection string
    used for async to connect to itself.  
  
  * self_concurrency INT DEFAULT 4: how many workers async is allowed to use
    to batch queries to itself.

Target: At least one target must be defined. A target object allows for the following 
configuration;
  * target TEXT (Required): the name of the target.

  * connection_string TEXT (required): the connection string used to connect to
    the target

  * max_workers INT DEFAULT 8: Each target creates a concurrency pool by
    default. That pool will only allow this many concurrently running tasks.

  * asynchronous_finish: BOOL DEFAULT false.   If a target is set to finish
    asynchronously, it is not considered compelte with the query invoking the 
    tasks resolves.  Rather, it must be manually marked complete with 
    async.finish().

Concurrency pool:
  If concurrency pools other than targets need to be set up, they can be passed
  in this way.  It is not required to do this unless the pool needs a custom
  maximum worker setting.  The concurrency pool object defines the following 
  fields:

  * concurrency_pool TEXT (Required): gives the name of the pool

  * max_workers INT (Required): sets the maximum number of workers.



Running tasks

* Tasks can be pushed to the orchestrator via:

CREATE OR REPLACE FUNCTION async.push_tasks(
  _tasks async.task_push_t[],
  _run_type async.task_run_type_t DEFAULT 'EXECUTE',
  _source TEXT DEFAULT NULL) RETURNS SETOF async.task AS

_tasks: an array of task_push_t. task_push_t offers the following record 
        to define each task sent.  Each record of the array will be a specific 
        task.

  task_push_t:
  (
    task_data JSONB DEFAULT NULL: an object to carry extra data with the task

    target TEXT (Required): the target the orchestator will connect to and run
                            the task against.

    priority INT DEFAULT 0: Tasks with a lower priority will always run before
                            tasks with a higher priority.  Priority < 0 is 
                            reserved for the orchestrator itself.

    query TEXT (Required): The query (task) to run

    concurrency_pool TEXT DEFUALT NULL: Use this pool to throttle task execution
                                        rather than the target.
  )

  The helper function, async.task() simplifies creation of task_push_t by
  making use of optional arguments:

CREATE OR REPLACE FUNCTION async.task(
  _query TEXT,
  target TEXT,
  task_data JSONB DEFAULT NULL,
  priority INT DEFAULT 0,
  concurrency_pool TEXT DEFAULT NULL) RETURNS async.task_push_t AS  



_run_type: allows fo the following:

  'EXECUTE':         run task normally

  'DOA':             place task into queue dead ('Dead On Arrival') not useful 
                     except when extending the library.  
         
  'EXECUTE_NOASYNC': run task, but do not set complete when it is invoked.
                     this basically overrides the target flag.

_source: An arbitrary text field to allow the caller to record who/what 
         initiated the task.

* Examples:

/* push a single 'select 0' on the queue against self */
SELECT async.push_tasks(array[async.task('select 0', 'SELF')]);

/* push a 1000 'select 0' queries on the queue against self */
SELECT async.push_tasks(array_agg(t))
FROM
( 
  SELECT async.task('select 0', 'SELF') 
  FROM generate_series(1,1000)
) q;




FAQ: 
  Q: What dependencies does async need to run?  Will it run on cloud managed
     platforms?
  A: Async depends on the dblink module heavily in order to do background 
     processing of queries. In theory, any platform that provides this module 
     should work.  pg_cron is also helpful since it can be used to host the 
     main() process. If this is not available, a client maintained process to
     connect to the database run main() might also be needed.

  Q: What is the minimum version of postgres async can run on?
  A: Async requries postgres 11, which is the version that introduced stored
     procedures.

  Q: My server ran out of memory? 
  A: JIT compilation settings have been known to leak memory in some 
     configurations.  If you are seeing memory leaks leading into a crash, try
     disabling JIT query optimization.

  Q: Can async run on its own databse or does it have to run on my normal 
     database?
  A: Asnyc is designed to run either inside an existing database or on 
     a separate database.  There are obvious benefits to both approaches 
     depending on circumstnaces.

  Q: How fast is async?  Aren't databases slow?
  A: Async ought to be table to faciltiate 100s to 1000s of tasks per second,
     perhaps into the 10s of thousands with good hardware.  It is not designed
     to be a zero latency messaging queue or scale to millions of concurrent 
     tasks.  Unless the tasks are extremly short, this should scale to a very
     large amount of work distributed across multiple servers and it is unlikely
     to be the bottlneck.  Async will benefit highly from fast cpu speeds, and
     will not need a lot of storage or memory unless a long task history is 
     kept.

  Q: Why build a query processing queue in a database? Is this sorcery? Are you 
     insane? Why not use an established orchesrtation engine such as airflow 
     instead?
  A: Orchestating postgres queries from postgres itself leads to simplicity and
     allows for deep exploitation of database mechanics.  Databases are highly
     concurrent engines that simplify some of the most important aspects of 
     develement, specifically concurrent management of data structures,
     fancy indexing strategies and other important considerations.  The author 
     feels that SQL is best suited for the problem domain given that most 
     queue/orchestration platforms log externally anways.  Also, the elegance
     of having the state machine and the log be the same data stucture is very 
     subtle but powerful once realized.

  Q: Why not package async as an extension?
  A: Postgres extensions have a major downside in that the presume the server
     is running on a platform with direct filesystem access.  In practical 
     terms, this makes extensions unsuitable for cloud hosted platforms such as
     RDS, GCP, and the like.  External package management is IMNHSO one of the 
     great unsolved problems of postgres.

  