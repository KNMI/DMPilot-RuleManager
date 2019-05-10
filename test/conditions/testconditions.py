def falseCondition(options, SDSFile):
    return False

def trueCondition(options, SDSFile):
    return True

def exceptionCondition(options, SDSFile):
    raise Exception("Oops!")

def optionCondition(options, SDSFile):

    return options["number"] == 10 and options["string"] == "string"
