import logging, logging.handlers, subprocess, os, os.path, sys
class Job:
	def __init__(self, command):
		self.command = command

PART_STRING = '--------------------------------------------------------------------------------------------'

#find the log path based on where this script is
dirname = os.path.dirname(sys.argv[0])
logPath = os.path.abspath(dirname) + '/log'
print 'logPath:' + logPath
if not os.path.exists(logPath):
	os.makedirs(logPath)

#setup logging
myLogger = logging.getLogger('MyLogger')
myLogger.setLevel(logging.DEBUG)
handler = logging.handlers.RotatingFileHandler(logPath + '/amino.log', maxBytes=5000000, backupCount=5)
myLogger.addHandler(handler)

jobList = []

#get job commands from jobList.py
from commandList import commands

previousReturnCodes = []
finalReturnCode = 0
#execute commands in order
for commandArray in commands:
	currentJobs = []

	#execute these commands at the same time
	for command in commandArray:
		
		#check to see if previous commands executed successfully
		for previousCode in previousReturnCodes:
			if command[1] and previousCode != 0:
				myLogger.debug('-------Not executing next command because the previous command executed with [' + str(previousCode) + '].-------')
				finalReturnCode = previousCode
		if finalReturnCode != 0:
			break
		
		#execute the command
		myLogger.debug(PART_STRING)
		myLogger.debug('Executing command [' + command[0] + ']')
		job = Job(command[0])
		jobList.append(job)
		proc = subprocess.Popen(command[0],
			shell=True,
			stdout=subprocess.PIPE,
			stderr=subprocess.STDOUT,
			)
		job.proc = proc
		currentJobs.append(job)
		myLogger.debug('PID [' + str(job.proc.pid) + ']')
		myLogger.debug(PART_STRING)
	if finalReturnCode != 0:
		break

	del previousReturnCodes[:]
	#wait for each job to finish and log the output
	for job in currentJobs:
		myLogger.debug(PART_STRING)
		myLogger.debug('Logging for command [' + job.command + '], PID [' + str(job.proc.pid) + ']')
		myLogger.debug(PART_STRING)
		while job.proc.poll() == None:
			line = job.proc.stdout.readline() #block, waiting for stdout
			if line != '':
				myLogger.debug(line.rstrip())

		while True:
			line = job.proc.stdout.readline()
			if line != '':
				myLogger.debug(line.rstrip())
			else:
				break
		
		myLogger.debug(PART_STRING)
		myLogger.debug("Exit code [" + str(job.proc.returncode) + "] for command [" + job.command + "]")
		myLogger.debug(PART_STRING)
		previousReturnCodes.append(job.proc.returncode)

#summarize job results and calculate return code
myLogger.debug(PART_STRING)
myLogger.debug('Job process summary:')
lastReturnCode = 0
for job in jobList:
	myLogger.debug("Exit code [" + str(job.proc.returncode) + "] for command [" + job.command + "]")
	lastReturnCode = job.proc.returncode

if lastReturnCode != 0:
	finalReturnCode = lastReturnCode

#return with the returnCode of the last command executed or the job that had an error
myLogger.debug(PART_STRING)
myLogger.debug('Exiting AMINO job execution script with exit code [' + str(finalReturnCode) + ']')
myLogger.debug(PART_STRING)
sys.exit(finalReturnCode)
