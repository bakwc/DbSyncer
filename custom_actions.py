from pysyncobj import replicated


class CustomActions(object):

	def __init__(self, driver, config):
		self.__config = config
		self.__driver = driver

	#
	#   Custom actions examples:
	#

	#
	# @replicated
	# def initTable(self):
	# 	self.__driver.executeWrite("CREATE TABLE person (name text, age real)")
	#
	# @replicated
	# def addPerson(self, name, age):
	# 	self.__driver.executeWrite("INSERT INTO person VALUES ('%s', %d)" % (name, age))
	#
	# def getPersons(self):
	# 	return self.__driver.executeRead("SELECT * FROM person")
	#
