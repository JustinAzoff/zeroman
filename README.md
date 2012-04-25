zeroman
=======

Kind of like gearman, but using zeromq

To test
=======

    zeromanager 1234&
    zeromanager 1235&

    python tests/worker_test.py&
    python tests/worker_test.py&

    python tests/client_test.py

TODO
====

 * implement async tasks.  The Client should be able to submit 10 jobs and then
   wait for 10 results. The worker shouldn't have to be changed, just the manager and the client.
   Likely need to add the concept of the 'task id'
