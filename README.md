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
