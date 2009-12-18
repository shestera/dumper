#!/usr/bin/env python
from __future__ import with_statement
import commands
import sys,os
from optparse import OptionParser
import ConfigParser
import logging
import glob
from stat import *
from time import strftime

def rel(*x):
    return os.path.join(os.path.abspath(os.path.dirname(__file__)), *x)

config = ConfigParser.ConfigParser()
config.read(rel('config.ini'))

class Dump:

    def __init__(self, raw=False, vars=None):
	self.conf = {}
	for option in config.options('config'):
            value = config.get('config', option, raw, vars)
            self.conf[option] = value
	self.conf['time_str'] = strftime("%Y%m%d_%H%M")
        self.dumpconf = ConfigParser.ConfigParser()
        self.dumpconf.read(rel(self.conf['inifile']))

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

    def nagios(self, raw=False, vars=None):
        commands = open(rel('template/nagios.tmp'), "r").read()
	host = open(rel('template/nagios_host.tmp'), "r").read()
	print host
        for section in self.dumpconf.sections():
            result = {}
            if section not in result:
                result[section] = {}
            for option in self.dumpconf.options(section):
                value = self.dumpconf.get(section, option, raw, vars)
                result[option] = value
            result['name'] = section
            print commands % result

    def startdump(self, job, raw=False, vars=None):
        for section in self.dumpconf.sections():
            result = {}
            if section not in result:
                result[section] = {}
            for option in self.dumpconf.options(section):
                value = self.dumpconf.get(section, option, raw, vars)
                result[option] = value
	    for jobs in result['job'].split():
				self.env = {}
				self.env = self.conf
				self.env['name'] = section
                		self.env['dir'] = '%s%s' % (self.env['dest_path'], section)

				if jobs is  job:
					if result['type'] == 'files':
						self.env['path'] = result['path']
						self.env['ext'] = self.env['archiver_ext']
		                		self.dump_files()
                    			elif result['type'] == 'database_pgsql':
						self.env['ext'] = self.env['dump_ext']
						self.env['host'] = result['host']
						self.env['user'] = result['user']
						self.env['database'] = result['database']
						self.env['port'] = result['port']
	                    			self.dump_database_pgsql()
	                		elif result['type'] == 'database_pgsql_table':
						self.env['ext'] = self.env['dump_ext']
                        			self.env['host'] = result['host']
                        			self.env['user'] = result['user']
                        			self.env['database'] = result['database']
                        			self.env['port'] = result['port']
						self.env['tables'] = result['tables']
                        			self.dump_database_pgsql_tables()

    def start(self, name, raw=False, vars=None):
        result = {}
        for option in config.options('%s' % name):
            value = config.get('%s' % name, option, raw, vars)
            result[option] = value

            self.env = {}
            self.env = self.conf
            self.env['name'] = section
            self.env['dir'] = '%s%s' % (self.env['dest_path'], section)

            if result['type'] == 'files':
                self.env['path'] = result['path']
                self.env['ext'] = self.env['archiver_ext']
                self.dump_files()
            elif result['type'] == 'database_pgsql':
                self.env['ext'] = self.env['dump_ext']
                self.env['host'] = result['host']
                self.env['user'] = result['user']
                self.env['database'] = result['database']
                self.env['port'] = result['port']
                self.dump_database_pgsql()
            elif result['type'] == 'database_pgsql_table':
                self.env['ext'] = self.env['dump_ext']
                self.env['host'] = result['host']
                self.env['user'] = result['user']
                self.env['database'] = result['database']
                self.env['port'] = result['port']
                self.env['tables'] = result['tables']
                self.dump_database_pgsql_tables()

	def zip(self, file):
		commands.getstatusoutput(self.compress_app + ' ' + file)

	def dump(self):
		ret = commands.getstatusoutput(self.env['c'])
		self.env['return_code'] = ret[0]
	        logging.info("dump status: %s" % ret[0])

	def send_nsca(self):
		return_code = '0'
		self.env['output'] = 'create %s - size %s' % (strftime("%Y-%m-%d %H:%M:%S"), os.path.getsize('%(dir)s/%(name)s_%(time_str)s%(ext)s' % self.env) )
		msg = ("backup\\t%(name)s\\t%(return_code)s\\t%(output)s\\n" % self.env)
		cmd = ("%(send_nsca)s -H %(nagios_server)s" % self.env)
		commands.getstatusoutput('printf "%s" | %s' % (msg,cmd))[0]
		logging.info("send nagios %(name)s" % self.env)

	def create_dir(self):
		try:
		    os.mkdir(self.env['dir'])
		    logging.info("created dir %(dir)s" % self.env)
		except OSError:
		    pass

	def symlink(self):
		try:
		    os.symlink('%(dir)s/%(name)s_%(time_str)s%(ext)s' % self.env, '%(dest_path)s%(name)s%(ext)s' % self.env)
        	    logging.info('create symlink %(dest_path)s%(name)s%(ext)s' % self.env)
        	except OSError:
            	    os.remove('%(dest_path)s%(name)s%(ext)s' % self.env)
            	    logging.info('remove symlink %(dest_path)s%(name)s%(ext)s' % self.env)
		    os.symlink('%(dir)s/%(name)s_%(time_str)s%(ext)s' % self.env, '%(dest_path)s%(name)s%(ext)s' % self.env)
		    logging.info('create symlink %(dest_path)s%(name)s%(ext)s' % self.env)

	def delete_old(self):
            names = glob.glob('%(dir)s/*' % self.env)
            for file in sorted(names)[:-10]:
			os.remove(file)
			logging.info("remove old backup %s " % file)

        def dump_database_pgsql(self):
		self.create_dir()
		c = '%(pg_dump)s %(pg_dump_options)s -h %(host)s -U %(user)s -f %(dir)s/%(name)s_%(time_str)s%(ext)s %(database)s' % self.env
		self.env['c'] = c
		logging.info("started %(name)s: %(c)s" % self.env)
                self.dump()
		self.symlink()
		self.delete_old()
		self.send_nsca()

	def dump_files(self):
		self.create_dir()
		c = '%(archiver)s %(archiver_options)s %(dir)s/%(name)s_%(time_str)s%(ext)s %(path)s' % self.env
		self.env['c'] = c
		logging.info("started %(name)s: %(c)s" % self.env)
		self.dump()
		self.symlink()
		self.delete_old()
		self.send_nsca()

        def dump_database_pgsql_tables(self):
		self.create_dir()
		tab = []
		for i in self.env['tables'].split():
			tab.append( ' -t %s ' % i)
		self.env['tab'] = ' '.join(tab)
		c = '%(pg_dump)s %(pg_dump_options)s -h %(host)s -U %(user)s %(tab)s -f %(dir)s/%(name)s_%(time_str)s%(ext)s %(database)s' % self.env
		self.env['c'] = c
		logging.info("started %(name)s: %(c)s" % self.env)
	        self.dump()
		self.symlink()
		self.delete_old()
		self.send_nsca()
		print self.env

def parse_setup():
	parser = OptionParser()
	parser.add_option("-l", "--listjob", dest="listjob", action="store_true")
	parser.add_option("-j", "--job", dest="job")
        parser.add_option("-c", "--cron", dest="crontab", action="store_true")
        parser.add_option("-n", "--nagios", dest="nagios", action="store_true")
        parser.add_option("-s", "--start", dest="start")
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
    elif options.nagios:
                dump.nagios()
    elif options.start:
		dump.start(options.start)
    else:
		dump.startdump(options.job)

if __name__ == "__main__":
	main()

