def assertQualityPolicy(options, SDSFile)
    """
    def assertQualityPolicy
    Asserts that the SDSFile quality is in options
    """
    return SDSFile.quality in options["qualities"]
