#!/usr/bin/env python
from __future__ import with_statement
import commands
import time
import sys,os
from optparse import OptionParser
import ConfigParser
import logging
import glob
from stat import *

def rel(*x):
	return os.path.join(os.path.abspath(os.path.dirname(__file__)), *x)

config = ConfigParser.ConfigParser()
config.read(rel('config.ini'))

class Dump:
 
	def __init__(self):
		current_time = time.localtime()
		self.time_str = str(current_time[0]) + str(current_time[1]) + str(current_time[2]) + '_' + str(current_time[3]) + str(current_time[4]) + str(current_time[5])
                self.compress_app = config.get('config', 'compress_app')
                self.archiver = config.get('config', 'archiver')
                self.archiver_options = config.get('config', 'archiver_options')
                self.pg_dump = config.get('config', 'pg_dump')
                self.pg_dump_options = config.get('config', 'pg_dump_options')
                self.dest_path = config.get('config', 'dest_path')
                self.compress_ext = config.get('config', 'compress_ext')
                self.archiver_ext = config.get('config', 'archiver_ext')
                self.inifile = config.get('config', 'iniFile')
                self.dump_ext = config.get('config', 'dump_ext')
                self.dumpconf = ConfigParser.ConfigParser()
                self.dumpconf.read(rel(self.inifile))

        def listjob(self, raw=False, vars=None):
                for section in self.dumpconf.sections():
                        result = {}
                        if section not in result:
                                result[section] = {}
                        for option in self.dumpconf.options(section):
                                value = self.dumpconf.get(section, option, raw, vars)
                                result[option] = value
			print result

	def crontab(self):
		for hourly in range(24):
			print "0 %s * * * %s -j %s" % (hourly, rel('dump.py'), hourly)


	def startdump(self, job, raw=False, vars=None):
                for section in self.dumpconf.sections():
                        result = {}
                        if section not in result:
                                result[section] = {}
                        for option in self.dumpconf.options(section):
                                value = self.dumpconf.get(section, option, raw, vars)
                                result[option] = value
			
			for jobs in result['job'].split():
				if jobs is  job:
					if result['type'] == 'files':
		                                self.dump_files(result['name'] ,result['path'])
                		        elif result['type'] == 'database_pgsql':
                                		self.dump_database_pgsql(result['name'], result['host'], result['user'], result['database'], result['port'])
                        		elif result['type'] == 'database_pgsql_table':
                                		self.dump_database_pgsql_tables(result['name'], result['host'], result['user'], result['database'], result['port'], result['tables'])

	def zip(self, file):
		commands.getstatusoutput(self.compress_app + ' ' + file)
 
	def dump(self, c):
		ret = commands.getstatusoutput(c)
 
	def symlink(self, filename, name, time_str, ext):
		try:
                    os.symlink('%s_%s%s' % (filename, time_str, ext), '%s%s' % (filename, ext))
                    logging.info("create symlink %s%s" % (filename, ext))
                except OSError:
                    link =  os.remove('%s%s' % (filename, ext))
                    logging.info("remove symlink %s%s " % (filename, ext))
                    os.symlink('%s_%s%s' % (filename, time_str, ext), '%s%s' % (filename, ext))
                    logging.info("create symlink %s%s" % (filename, ext))

	def delete_old(self, filename):
                names = glob.glob('%s_*' % filename)
                for file in sorted(names)[:-10]:
			os.remove(file)
			logging.info("remove old backup %s " % (file))


        def dump_database_pgsql(self, name, host, user, database, port):
                filename = '%s%s' % (self.dest_path, name)
		c = '%s %s -h %s -U %s -f %s_%s.dump %s' % (self.pg_dump, self.pg_dump_options, host, user, filename, self.time_str, database)
		logging.info("started %s: %s" % (name, c))
                self.dump(c)
		self.symlink(filename, name, self.time_str, self.dump_ext)
		self.delete_old(filename)

	def dump_files(self, name, path):
		filename = '%s%s' % (self.dest_path, name)
		c = '%s %s %s_%s%s %s' % (self.archiver, self.archiver_options, filename, self.time_str, self.archiver_ext, path)
		logging.info("started %s: %s" % (name, c))
		self.dump(c)
		self.symlink(filename, name, self.time_str, self.archiver_ext)
		self.delete_old(filename)

        def dump_database_pgsql_tables(self, name, host, user, database, port, tables):
                filename = '%s%s' % (self.dest_path, name)
		tab = []
		for i in tables.split():
			tab.append( ' -t %s ' % i)
		tab = ' '.join(tab)
                c = '%s %s -h %s -U %s%s-f %s_%s.dump %s' % (self.pg_dump, self.pg_dump_options, host, user, tab,  filename, self.time_str, database)
                self.dump(c)
		self.symlink(filename, name, self.time_str, self.dump_ext)
		self.delete_old(filename)

def parse_setup():
	parser = OptionParser()
	parser.add_option("-l", "--listjob", dest="listjob", action="store_true")
	parser.add_option("-j", "--job", dest="job")
        parser.add_option("-c", "--cron", dest="crontab", action="store_true")
	(options, arg) = parser.parse_args()
	return (options, arg)
 
def main():
	(options, arg) = parse_setup()
        logfile = config.get('config', 'logfile')
        FORMAT = "%(asctime)-15s %(levelname)s %(message)s"
        logging.basicConfig(level=logging.DEBUG, filename=logfile,filemode='a', format=FORMAT, datefmt='%d %b %Y %H:%M:%S',)

	dump = Dump()

	if options.listjob:
		dump.listjob()
	elif options.crontab:
		dump.crontab()
	else:
		dump.startdump(options.job)
 
if __name__ == "__main__":
	main()
