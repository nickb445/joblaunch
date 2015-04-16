# Description #
joblaunch is a parallel job scheduler. It is used to schedule jobs in a multithreads environment, where each job must wait for others to finish before being launched. Each job consists of one or more commands. A command can have anything that can be interpreted by the shell, e.g. pipes, redirections, etc.

It has been tested on Linux and Windows with Python 2.6.

# File syntax #

The jobs and their dependencies are indicated in a text file with the following format:

```
<number of jobs>
<command>*
<dependency>*
```

where:
  * `<number of jobs>` is a positive integer to indicate the number of jobs to run.
  * `<command>` is a job name followed by a command. The name can be any combination of numbers, letters and punctuation marks (no spaces). The command is executed **as is** through the shell (it can have redirections, expansions, multiple expressions, ...)
  * `<dependency>` is a list of job names `(JA, JB1, ..., JBn)` separated by spaces. It indicates that job `JA` depends on jobs `JB1, ..., JBn` (that is, `JA` can't be executed until all `JBx` have finished).

## Example ##

File jobs.jb:
```
3
job_a sleep 1; echo a
job_b sleep 1; echo b
job_c sleep 1; echo c
job_a job_c
```

In the example we have 3 jobs with names `job_a`, `job_b` and `job_c`. `job_a` must wait for `job_c` to finish.

We can launch the above jobs using 2 threads and logging messages to file `log` this way:

```
joblaunch.py -n 2 -l log jobs.jb
```

The path of the file that describes the jobs and their dependencies is given as the last argument. If this path is `-`, the information is read from standard input.