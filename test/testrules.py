import time

def timeoutRule(options, SDSFile):

    time.sleep(6)

def exceptionRule(options, SDSFile):

    raise Exception("Oops!")

def passRule(options, SDSFile):

    pass

def optionRule(options, SDSFile):

    if options["number"] != 10 or options["string"] != "string":
        raise Exception("Oops!")
