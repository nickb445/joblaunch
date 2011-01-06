#!/usr/bin/python

__version__ = "0.11"
__autor__ = "Yassin Ezbakhe <yassin@ezbakhe.es>"

import sys
import time
import optparse
import logging
import subprocess
import threading
import multiprocessing
import collections
import Queue

class Graph:
    """
    Directed graph class

    We have our own implementation because we don't need all the functionality
    offered by other packages, such as NetworkX.
    """

    (WHITE, GREY, BLACK) = (0, 1, 2)

    def __init__(self):
        """
        Initialize an empty graph.
        """
        self.nodes = set()
        self.numNodes = 0
        self.edges = { }

    def addNode(self, node):
        """
        Add a node to the graph. If the node already exists, do nothing.
        """
        if not node in self.nodes:
            self.nodes.add(node)
            self.numNodes += 1
            self.edges[node] = set()

    def addEdge(self, u, v):
        """
        Add an egde between two nodes in the graph. The nodes are created if
        they don't exist.
        """
        self.addNode(u)
        self.addNode(v)
        self.edges[u].add(v)

    def containsCycle(self):
        """
        Return True if the graph contains a cycle. In our case, a task graph
        that has a cycle with lead to a deadlock.
        """
        # http://www.eecs.berkeley.edu/~kamil/teaching/sp03/041403.pdf
        # In order to detect cycles, we use a modified depth first search
        # called a colored DFS. All nodes are initially marked WHITE. When a
        # node is encountered, it is marked GREY, and when its descendants
        # are completely visited, it is marked BLACK. If a GREY node is ever
        # encountered, then there is a cycle.
        marks = dict([(v, Graph.WHITE) for v in self.nodes])
        def visit(v):
            marks[v] = Graph.GREY
            for u in self.edges[v]:
                if marks[u] == Graph.GREY:
                    return True
                elif marks[u] == Graph.WHITE:
                    if visit(u):
                        return True
            marks[v] = Graph.BLACK
            return False
        for v in self.nodes:
            if marks[v] == Graph.WHITE:
                if visit(v):
                    return True
        return False

    def __iter__(self):
        for u in sorted(self.nodes):
            for v in sorted(self.edges[u]):
                yield (u, v)

class AtomicCounter:
    """
    Counter that implements atomic operations to be used in multiple threads.
    For now, the only operation we need is decrement.
    """

    def __init__(self, value):
        self.value = value
        self.lock = threading.Lock()

    def decrement(self):
        """
        Decrement the counter and return True if the decremented value is >= 0
        """
        with self.lock:
            res = self.value > 0
            self.value -= 1
        return res

    def __repr__(self):
        return str(self.value)

class PriorityQueue(Queue.PriorityQueue):
    """
    Priority queue. The priority is the property 'priority' of the item
    """

    def put(self, item):
        Queue.PriorityQueue.put(self, (item.priority, item))

    def get(self):
        (priority, item) = Queue.PriorityQueue.get(self)
        return item

class OutputFileWriter:
    """
    This clases manages the file where the output of each job is written.
    """

    lock = threading.Lock()

    def __init__(self, outputFilePath, verbose):
        fout = open(outputFilePath, "w") if outputFilePath != "-" else sys.stdout
        self.outputFile = fout
        self.verbose = verbose

    def write(self, jobId, stdout):
        """
            Write the standard output of a job to the file.
            If self.verbose is True, the job id is also printed and the
            output is tabbed to the right.
        """

        if self.verbose:
            # this is very inefficient if there are too much lines to write
            output = [ "%s\n" % jobId ]
            # tab each line two spaces to the right
            output.extend("  " + line for line in stdout.readlines())
            output.append("\n")
        else:
            output = stdout.readlines()

        with OutputFileWriter.lock:
            self.outputFile.writelines(output)

    def close(self):
        self.outputFile.close()

class Job:
    """
    Class that encapsulates a job. Each job consists of an id, a priority
    and a list of commands. This class is responsible of launching
    each of the commands and writing the output and the log to a file.
    """

    outputFileWriter = None    

    def __init__(self, id_, priority, commandsList):
        self.id = id_
        self.priority = priority
        self.commands = commandsList
        assert len(self.commands) > 0
        self.predecessors = set()
        self.successors = set()

    def __call__(self):
        assert not self.predecessors
        logging.info("%s started %s", self.id, threading.currentThread().name)
        begin = time.time()
        elapsed = self.__launch()
        end = time.time()
        logging.info("%s finished %f", self.id, end - begin)

    def __repr__(self):
        return "Job %s" % self.id

    def __launch(self):
        # IMPORTANT: In UNIX, Popen uses /bin/sh, whatever the user shell is
        for cmd in self.commands:        
            p = subprocess.Popen(cmd, shell = True, stdout = subprocess.PIPE,
                stderr = subprocess.STDOUT)
            ret = p.wait()
            Job.outputFileWriter.write(self.id, p.stdout)
        return ret

class Core(threading.Thread):
    """
    Each object of this class is a thread that schedules jobs to be
    scheduled when they are ready.
    """

    def __init__(self, queue, numJobsLeft):
        super(Core, self).__init__()
        self.queue = queue
        self.numJobsLeft = numJobsLeft

    def run(self):
        """
        Run in a thread while there are jobs left
        """
        while self.numJobsLeft.decrement():
            job = self.queue.get()
            job()
            self.__addPreparedJobsToQueue(job)

    def __addPreparedJobsToQueue(self, job):
        """
        When a job has finished its execution, this method is called so as
        to put in the queue the jobs that were waiting for it (sucessors) and
        are not waiting for any other job.
        """
        for succ in job.successors:
            succ.predecessors.remove(job)
            if not succ.predecessors:
                self.queue.put(succ)

def check(condition, errorMsg):
    """
    Simple error handler. If the condition is False, write the error to
    console and abort program execution.
    """
    if not condition:
        sys.stderr.write("%s: error: %s\n" % (sys.argv[0], errorMsg))
        sys.exit(1)

def parseInput(inputFilePath):
    """
    Open the input file, parse it and return the dependency graph,
    the dictionary with the commands lists and the dictionary with the ids
    for each job.

    Input file must have the following format:

    <number of jobs>
    <command>*
    <dependency>*

    where:

        - <number of jobs> is a positive integer to indicate the number of jobs
          to run.
        - <command> is a job name followed by a command. The name can be any
          combination of numbers, letters and punctuation marks (no spaces).
          The command is executed as is through the shell (it can have
          redirections, pipes, expansions, multiple expressions, ...).
        - <dependency> is a list of job names (JA, JB1, ..., JBn) separated by
          spaces. It indicates that job JA depends on jobs JB1, ..., JBn
          (that is, JA can't be executed until all JBx have finished).
    """

    def parseDependencies(text):
        """
        Parse the dependencies part and return a dependency graph, where there
        is an edge between nodes JB and JA if JA depends on JB.
        """
        G = Graph()
        for line in text:
            # tokens = [ JA, JB1, ..., JBn ]
            tokens = line.strip().split()
            check(len(tokens) >= 2, "input file has an invalid syntax")
            JA = tokens[0]
            for JB in tokens[1:]:
                G.addEdge(JB, JA)
        return G

    def parseCommands(text):
        """
        Parse the commands part and return a dictionary, where entry J
        is a list with the commands for job J.
        """
        commands = collections.defaultdict(list)
        for line in text:
            (job, cmd) = line.strip().split(None, 1)
            commands[job].append(cmd)
        return commands

    def getJobsOrder(text):
        """
        Return ids for jobs (the ordering is the same as the used in the input
        file) and return a dictionary where entry J is the id of job J.
        """
        jobsOrder = { }
        id_ = 0
        for line in text:
            (job, _) = line.strip().split(None, 1)
            if job not in jobsOrder:
                jobsOrder[job] = id_
                id_ += 1
        return jobsOrder

    # open and read input file
    try:
        f = open(inputFilePath) if inputFilePath != "-" else sys.stdin
        lines = f.readlines()
        check(len(lines) > 0, "input file is empty")
        f.close()
    except IOError as e:
        check(False, e.strerror)
    
    # split file
    numJobs = abs(int(lines[0]))
    commandsText = lines[1:numJobs + 1]
    dependenciesText = lines[numJobs + 1:]

    # parse file
    commands = parseCommands(commandsText)
    dependencies = parseDependencies(dependenciesText)
    jobsOrder = getJobsOrder(commandsText)

    return (commands, dependencies, jobsOrder)

def createJobs(commands, G, jobsOrder):
    """
    Create a Job object for each job, filling the successors and predecessors
    lists, and return the jobs dictionary, where the key is the id
    and the value is the Job object.
    """

    jobs = { }
    def getJob(job):
        # create a new job or return the existing one
        if job not in jobs:
            jobs[job] = Job(job, jobsOrder[job], commands[job])
        return jobs[job]

    for (u, v) in G:
        uJob = getJob(u)
        vJob = getJob(v)
        uJob.successors.add(vJob)
        vJob.predecessors.add(uJob)

    # add jobs not listed in the dependency list (jobs that don't depend on
    # other jobs nor others depend on them)
    for job in commands:
        if job not in jobs:
            jobs[job] = Job(job, jobsOrder[job], commands[job])

    assert len(commands) == len(jobs)

    return jobs

def createQueue(jobs):
    """
    Create and return the initial job queue with jobs that don't depend
    on any other.
    """
    q = PriorityQueue()
    for t in jobs:
        if not jobs[t].predecessors:
            q.put(jobs[t])
    check(not q.empty(), "no initial job found to launch due to dependency cycles")
    return q

def start(commands, G, jobsOrder, numThreads):
    """
    Launch threads and begin working. The function waits for all jobs to end.
    """

    jobs = createJobs(commands, G, jobsOrder)
    jobsQueue = createQueue(jobs)
    numJobsLeft = AtomicCounter(len(jobs))

    cores = [ ]
    for i in range(numThreads):
        core = Core(jobsQueue, numJobsLeft)
        cores.append(core)
        core.start()
    for core in cores:
        core.join()

def getCPUCount():
    """
    Return the number of cores in the machine.
    """
    return multiprocessing.cpu_count()

def parseArguments(args):
    """
    Parse program arguments and return options used by user.
    """

    usage = "%prog [options] inputFile"
    version = "%%prog %s" % __version__
    description = ( \
        "%prog is a parallel job scheduler. It is used to schedule jobs in a "
        "multithreads environment, where each job must wait for others to "
        "finish before being launched. Each job consists of one or more "
        "commands. A command can have anything that can be interpreted by the "
        "shell, e.g. pipes, redirections, etc.")
    epilog = "Written by Yassin Ezbakhe <yassin@ezbakhe.es>"

    parser = optparse.OptionParser(usage = usage, version = version,
        description = description, epilog = epilog)

    parser.add_option("-n", "--numThreads",
                      action = "store", type = "int", dest = "numThreads",
                      default = getCPUCount(),
                      help = "maximum number of threads to run concurrently "
                             "[default: %default]")
#    parser.add_option("-i", "--input-file",
#                      action = "store", dest = "inputFile", default = "-",
#                      help = "read jobs from INPUTFILE (if -, read from "
#                             "standard input) [default: %default]")
    parser.add_option("-l", "--log-file",
                      action = "store", dest = "logFile",
                      help = "log all messages to LOGFILE")
    parser.add_option("-o", "--output-file",
                      action = "store", dest = "outputFile", default = "-",
                      help = "redirect all jobs stdout and stderr to OUTPUTFILE "
                             "(if -, redirect to standard output) "
                             "[default: %default]")
    parser.add_option("--force",
                      action = "store_true", dest = "force", default = False,
                      help = "force execution of jobs without checking for "
                             "dependency cycles (MAY CAUSE DEADLOCKS!)")
    parser.add_option("-v", "--verbose",
                      action = "store_true", dest = "verbose", default = False,
                      help = "turn on verbose output")

    (options, args) = parser.parse_args(args)
    
    # use stdin if no inputFile is given as argument
    options.inputFile = args[0] if len(args) > 0 else "-"

    return options

def main():
    options = parseArguments(sys.argv[1:])
    inputFile = options.inputFile

    if options.logFile:
        logging.basicConfig(level = logging.INFO,
                            filename = options.logFile,
                            format = "%(asctime)s %(message)s",
                            datefmt = "%Y-%m-%d %H:%M:%S")

    # set output writer
    Job.outputFileWriter = OutputFileWriter(options.outputFile, options.verbose)

    # parse input file
    (commands, G, jobsOrder) = parseInput(inputFile)

    # check that there are no dependency cycles
    if not options.force:
        check(not G.containsCycle(), "there are dependency cycles (use --force)")

    # check that all jobs have a command
    check(all(job in commands for job in G.nodes), "some jobs don't have a command")

    # begin working
    start(commands, G, jobsOrder, options.numThreads)

if __name__ == "__main__":
    if sys.version_info < (2, 6):
        error("Python >= 2.6 is required")

    try:
        main()
    except Exception as e:
        check(False, e)

