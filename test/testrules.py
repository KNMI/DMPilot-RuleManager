import time

def timeoutRule(SDSFile, options):

    time.sleep(6)

def exceptionRule(SDSFile, options):

    raise Exception("Oops!")

def passRule(SDSFile, options):

    pass
