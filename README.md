# RDSA Data Manager

## rulemanager.py

Main entry point for processing.

## rules/

Init script with rule descriptions. All rules that are configured in _rules.json_
will be called with two parameters that are passed (SDSFile, options) which are
the file object and rule-specific options respectively.

## Running it

1) Edit the `configuration.py` file with options needed to run the script, e.g.,
the location of the files, and the information to connect to iRODS and the Mongo
database.

2) Edit the `rules.json` file specifying which rules to run, in what order, and
with which options.

3) Run `rulemanager.py`.

## Implementing a new rule

Create a new method in the `RuleFunctions` class with the following signature:
`exampleRule(self, options, sdsFile)`. The first argument, `options` will be a
`dict` with the options passed through `rules.json`. The second argument,
`sdsFile` is a `SDSFile` object describing the file.

To include the new `exampleRule` in the execution, add a new object to the list
in `rules.json` pointing to it, and defining its options, like so:
```
{
    'name': 'exampleRule',
    'options': { ... }
}
```