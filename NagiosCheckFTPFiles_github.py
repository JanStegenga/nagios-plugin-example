#!/usr/bin/python3

import nagiosplugin
import datetime
import pandas as pd
import argparse
import os
import tarfile
from   ftplib import FTP
import io

#---- 
ftpaddress    = ''
loginname     = ''
loginpassw    = ''
ftpfiledir	  = ''

#---- functions to get data in
def read_file( file ):
	#header of the file is of the form:
	# 0 file information line
	# 1 file information line
	# 2 <ITEM_NAME>, [<UNIT>]: <VALUE> \n
	# ....
	# 19 <ITEM_NAME>, [<UNIT>]: <VALUE> \n
	# 20 to EOF: other data - not used here...
	#
	#read the file; create a DATAFRAME type. indexing by position is done by iloc[.,.]
	header_raw = pd.read_csv(file, skiprows = 2, names=['ITEM', 'UNIT', 'VALUE'], sep=',|:', nrows=17, skipinitialspace = True, ) #.iloc[:,0]    
	#resolve inconsistencies in XXXXX files, where the units are not always present:
	idx = header_raw.VALUE.isnull()
	header_raw.VALUE[ idx ] = header_raw.UNIT[ idx ]
	header_raw.UNIT[ idx ] = '[]'
	header_raw.set_index( 'ITEM', inplace=True )
	return header_raw	
	
def FilesLast24H( ):	
	#
	id = 1								
	# list all files on ftp server 
	ftp = FTP( ftpaddress)
	ftp.login( loginname, loginpassw )
	ftp.cwd( ftpfiledir )
	## only use files > 100000 bytes 
	a = []
	remotefilenames = []
	for item in ftp.mlsd(facts=['size']):
		if 'size' in list(item[1].keys() ):			# not true for directories
			a.append( [item[1]['size'],item[0]] )
			remotefilenames.append( item[0] )
	a.sort( key = lambda x: x[1])					# sort by name
	remotefilenames.sort()							# sort by name
	
	b = [aa[1] for aa in a if int(aa[0])>100000]	# names of files > 100000 bytes
	c = sorted( b )
	latestspecfile 			= c[-1]	
	onebutlatestspecfile 	= c[-2]
	#
	latestfile 	= [x for x in remotefilenames if '.tar.xz' in x ][-1]
	toa 		= [ datetime.datetime.fromtimestamp(int(filename[2:-7])) for filename in remotefilenames if '.tar.xz' in filename]
	bools		= [ x > datetime.datetime.now() - datetime.timedelta( hours = 24 ) for x in toa ]
	tslu 		= round( ( datetime.datetime.now() - toa[-1] ).total_seconds() )
	
	#read the file here as well
	[filetype, no_spec_files, voltage, diskused, error] = readfileheader_inmemory( ftp, latestspecfile )
	if error != 'none':
		[filetype, no_spec_files, voltage, diskused, error] = readfileheader_inmemory( ftp, onebutlatestspecfile )
	if error != 'none':
		print( error )
	yield id, tslu, latestfile, latestspecfile, sum( bools ), voltage, diskused  

def readfileheader_inmemory( ftp, filename ):
	error = 'none'
	finmem = io.BytesIO()										#retrieve file into a byteStream object
	ftp.retrbinary( 'RETR ' + filename, finmem.write)			
	finmem.seek(0)												#put pointer at start
	tar = tarfile.open( fileobj=finmem, mode='r:xz' )			#'r:xz' -> seek-able
	allfiles = []
	try:
		allfiles	= [tarinfo.name for tarinfo in tar]
	except Exception as inst:
		error = str( inst ) + ' found in ' + filename + '\n'
		
	spec_files 	= ['1.txt', '2.txt', '3.txt', '4.txt', '5.txt', '6.txt' ]	#these files are expected 
	boollist 	= [ any( [x in y for y in allfiles] ) for x in spec_files ]	#actually present?
	no_spec_files = sum( boollist )

	if no_spec_files > 0:
		#access a single file (the headers were all the same in the project)
		thisfile = [i for i, y in enumerate( allfiles ) if spec_files[boollist.index(True)] in y][0]
		fobj2    = tar.extractfile( tar.members[thisfile] )					#extract to a file-like object
		df 		 = read_ecowatch( io.StringIO( fobj2.read().decode() ) )	#pandas can handle a StringIO object instead of a true file
		voltage  = float( df.loc['BATTERY VOLTAGE']['VALUE'] )/100			#voltage is an integer 100 times the actual value
		diskused = ( df.loc['DISK USAGE']['VALUE'] ).split( '/' )[0]		#disk usage is of type: '129K / 10M'
		diskused = float( diskused[:-1] ) if diskused[-1] == 'M' else float( diskused[:-1] )/1024
		filetype = 'spectra measurement'
	else:
		voltage  = -1
		filetype = 'no spectra'
		diskused = -1
		
	tar.close()
	return filetype, no_spec_files, voltage, diskused, error

#---- classes/functions to do the nagios-thing
class PythonChecks( nagiosplugin.Resource ):
	def __init__( self ):
		#retrieve latest file; will generate only one result with id = 1
		self.resultgenerator = FilesLast24H()
			
	def probe( self ):
		for items in self.resultgenerator:
			yield nagiosplugin.Metric('node %d Update' % items[0], items[1], min=0, context='seconds')
			yield nagiosplugin.Metric('node %d Count ' % items[0], items[4], min=0, context='counts')
			yield nagiosplugin.Metric('node %d Volt  ' % items[0], items[-2], min=0, context='volts')
			yield nagiosplugin.Metric('node %d MB    ' % items[0], items[-1], min=0, context='MBs')

class PythonChecksSummary( nagiosplugin.Summary ):
	def __init__(self):
		pass
	
	def ok(self, results):
		print( '\n'.join( [ str(r) for r in results ] ) )
		return 'checks: '
	
def main():
	'''
	range definitions:
	10:20	-> generate alert if outside [10,20] and lower than 0
	@10:20	-> generate alert if inside [10:20]
	'''
	argp = argparse.ArgumentParser(description=__doc__)
	argp.add_argument('-t', '--type', metavar='FUNCTION', default='PythonChecks', help='name of quantity to be checked')
	argp.add_argument('-r', '--warningsecond', metavar='RANGE', default='', help='return warning if time since last update is outside RANGE')
	argp.add_argument('-s', '--criticalsecond', metavar='RANGE', default='', help='return critical if time since last update is outside RANGE')
	argp.add_argument('-u', '--warningvolt', metavar='RANGE', default='', help='return warning if voltage is outside RANGE')
	argp.add_argument('-v', '--criticalvolt', metavar='RANGE', default='', help='return critical if voltage is outside RANGE')
	argp.add_argument('-b', '--warningcount', metavar='RANGE', default='', help='return warning if count is outside RANGE, count = number of files in last 24H')
	argp.add_argument('-c', '--criticalcount', metavar='RANGE', default='', help='return critical if count is outside RANGE, count = number of files in last 24H')
	argp.add_argument('-f', '--warningdiskused', metavar='RANGE', default='', help='return warning if diskused is outside RANGE')
	argp.add_argument('-g', '--criticaldiskused', metavar='RANGE', default='', help='return critical if diskused is outside RANGE')
	
	#argp.add_argument('-r', '--percpu', action='store_true', default=False)
	args = argp.parse_args()
	checkPythonChecks = nagiosplugin.Check(
		PythonChecks(),
		nagiosplugin.ScalarContext('seconds', 	args.warningsecond, args.criticalsecond),
		nagiosplugin.ScalarContext('volts', 	args.warningvolt, args.criticalvolt),
		nagiosplugin.ScalarContext('counts',	args.warningcount, args.criticalcount),
		nagiosplugin.ScalarContext('MBs', 		args.warningdiskused, args.criticaldiskused),
		PythonChecksSummary()
		)

	if args.type == "PythonChecks":
		checkPythonChecks.main()
	else:
		print( 'unrecognized argument:' + str( args.type ) )
	
'''
Usage:
	This script can be called on a nagios core server. Easiest is to make a git directory on that machine 
	which holds all such tests and the configuration files as well. The script attempts to connect to an
	FTP server and checks the '*.tar.xz' files there. These files are expected to hold several .txt files
	which have in the header a value for voltage and disk space. In the project for which it was developed
	a solar powered remote measurement device put a fixed number of files on the server every day (by M2M).
	
	In the nagios framework add to services.cfg and commands.cfg:
	  services.cfg:
		define service {
			use                     generic-service
			host_name               check_data_on_ftp_server
			check_interval          30
			max_check_attempts      2
			service_description     check_data_on_ftp_server_py_update
			check_command           check_data_on_ftp_server_py_update!PythonChecks!4320000!8640000!10:!9:!100:!50:!24:!40:
			}

	  commands.cfg:
		define command {
			command_name     check_data_on_ftp_server_py_update
			command_line     /<path>/NagiosCheckFTPFiles.py -t $ARG1$ -r $ARG2$ -s $ARG3$ -u $ARG4$ -v $ARG5$ -b $ARG6$ -c $ARG7$ -f $ARG6$ -g $ARG7$
			}
'''

	

	
if __name__ == '__main__':
	import warnings
	warnings.filterwarnings('ignore')
	main()

	
	
	
	