#!/usr/bin/python

__version__ = "0.1"
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
    """Directed graph"""
    (WHITE, GREY, BLACK) = (0, 1, 2)
    def __init__(self):
        self.nodes = set()
        self.numNodes = 0
        self.edges = { }
    def addNode(self, node):
        if not node in self.nodes:
            self.nodes.add(node)
            self.numNodes += 1
            self.edges[node] = set()
    def addEdge(self, u, v):
        self.addNode(u)
        self.addNode(v)
        self.edges[u].add(v)
    def containsCycle(self):
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
    def __init__(self, value):
        self.value = value
        self.lock = threading.Lock()
    def decrement(self):
        with self.lock:
            res = self.value > 0
            self.value -= 1
        return res
    def __repr__(self):
        return str(self.value)

class PriorityQueue(Queue.PriorityQueue):
    """
    priority queue. the priority is the property 'priority' of the item
    """
    def put(self, item):
        Queue.PriorityQueue.put(self, (item.priority, item))
    def get(self):
        (priority, item) = Queue.PriorityQueue.get(self)
        return item

class OutputFileWriter:
    lock = threading.Lock()
    def __init__(self, outputFilePath, verbose):
        fout = open(outputFilePath, "w") if outputFilePath != "-" else sys.stdout
        self.outputFile = fout
        self.verbose = verbose
    def write(self, jobId, stdout):
        if self.verbose:
            # this is very inefficient if there are too much lines to write
            # however, it isn't the normal usage case
            output = [ "%s\n" % jobId ]
            output.extend("  " + line for line in stdout.readlines())
            output.append("\n")
        else:
            output = stdout.readlines()
        with OutputFileWriter.lock:
            self.outputFile.writelines(output)
    def close(self):
        self.outputFile.close()

class Job:
    outputFileWriter = None    
    def __init__(self, id_, priority, commands):
        self.id = id_
        self.priority = priority
        self.commands = commands
        assert len(commands) > 0
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
    def __init__(self, queue, numJobsLeft):
        super(Core, self).__init__()
        self.queue = queue
        self.numJobsLeft = numJobsLeft
    def run(self):
        while self.numJobsLeft.decrement():
            job = self.queue.get()
            job()
            self.__addPreparedJobsToQueue(job)
    def __addPreparedJobsToQueue(self, job):
        for succ in job.successors:
            succ.predecessors.remove(job)
            if not succ.predecessors:
                self.queue.put(succ)

def check(condition, errorMsg, *args):
    if not condition:
        sys.stderr.write("%s: error: %s\n" % (sys.argv[0], errorMsg % args))
        sys.exit(1)

def parseInput(inputFilePath):
    def parseDependencies(text):
        G = Graph()
        for line in text:
            tokens = line.strip().split()
            check(len(tokens) >= 2, "input file has an invalid syntax")
            u = tokens[0]
            for v in tokens[1:]:
                G.addEdge(v, u)
        return G
    def parseCommands(text):
        commands = collections.defaultdict(list)
        for line in text:
            (job, cmd) = line.strip().split(None, 1)
            commands[job].append(cmd)
        return commands

    def getJobsOrder(text):
        jobsOrder = { }
        id_ = 0
        for line in text:
            (job, _) = line.strip().split(None, 1)
            if job not in jobsOrder:
                jobsOrder[job] = id_
                id_ += 1
        return jobsOrder

    try:
        f = open(inputFilePath) if inputFilePath != "-" else sys.stdin
        lines = f.readlines()
        check(len(lines) > 0, "input file is empty")
        f.close()
    except IOError as e:
        check(False, e.strerror)
    
    numJobs = abs(int(lines[0]))
    commandsText = lines[1:numJobs + 1]
    dependenciesText = lines[numJobs + 1:]

    commands = parseCommands(commandsText)
    dependencies = parseDependencies(dependenciesText)
    jobsOrder = getJobsOrder(commandsText)

    return (commands, dependencies, jobsOrder)

def createJobs(commands, G, jobsOrder):
    jobs = { }
    def getJob(job):
        if job not in jobs:
            jobs[job] = Job(job, jobsOrder[job], commands[job])
        return jobs[job]

    for (u, v) in G:
        uJob = getJob(u)
        vJob = getJob(v)
        uJob.successors.add(vJob)
        vJob.predecessors.add(uJob)

    # add jobs not listed in the dependency list (isolated nodes in G)
    for job in commands:
        if job not in jobs:
            jobs[job] = Job(job, jobsOrder[job], commands[job])

    assert len(commands) == len(jobs)

    return jobs

def createQueue(jobs):
    q = PriorityQueue()
    for t in jobs:
        if not jobs[t].predecessors:
            q.put(jobs[t])
    check(not q.empty(), "no initial job found to launch due to dependency cycles")
    return q

def start(commands, G, jobsOrder, numThreads):
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
    return multiprocessing.cpu_count()

def parseArguments(args):
    usage = "%prog [options] inputFile"
    version = "%%prog %s" % __version__
    description = ( \
        "%prog is a shell tool for executing interdependent jobs in parallel "
        "locally. A job is launched only if all the jobs that it depends on "
        "have finished their execution.")
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

    (commands, G, jobsOrder) = parseInput(inputFile)

    # check that there are no dependency cycles
    if not options.force:
        check(not G.containsCycle(), "there are dependency cycles (use --force)")

    # check that all jobs have a command
    check(all(job in commands for job in G.nodes), "some jobs don't have a command")

    start(commands, G, jobsOrder, options.numThreads)

if __name__ == "__main__":
    if sys.version_info < (2, 6):
        error("Python >= 2.6 is required")

    try:
        main()
    except Exception as e:
        check(False, e)

