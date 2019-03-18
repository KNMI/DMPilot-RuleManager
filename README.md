# RDSA Data Manager

This project provides two main products, an SDS policy manager, and a
generic framework to define new policy managers.

First, an SDS policy manager, launched by the script in
`sdsmanager.py`. This is an extensible policy manager, that is able to
run a sequence of policies/rules on a list of SDS daily files. Its
objective is to be easy to configure, through the configuration of the
existing rules, and also easy to extend, with the addition of new
policies.

Second, the `RuleManager` class, that is the base to the SDS manager,
but is also able to transparently apply a sequence of policies to a
collection of data items of different types, as long as the policies
accept these types.

## Dependencies

The Rule Manager uses many data tools from IRIS that need to be compiled and
added to `$PATH`. These programs include:

- IRIS Dataselect (https://github.com/iris-edu/dataselect)
- IRIS MSRepack (https://github.com/iris-edu/libmseed)
- IRIS MSI (https://github.com/iris-edu/msi)

Source compilation instructions can be found on the respective pages.

## Running the SDS policy manager

1) Edit the `configuration.py` file with options needed to run the script, e.g.,
the location of the files, and the information to connect to iRODS and the Mongo
database.

2) Edit your JSON rule map file specifying which rules to run, in what order, and
with which options. The rules available are the ones in `rules.sdsrules`.

3) Run `python3 sdsmanager.py --dir /path/to/archive --rulemap rules.json`.

## Implementing a new rule for an existing manager

Create a new top-level function in the module being used by the
manager with the following signature: `exampleRule(options,
item)`. The first argument, `options` will be a `dict` with the
options passed through the JSON rule configuration. The second
argument, `item` is the object to which the rule is applied.

For example, rules for the SDS archive are in the `sdsrules` module,
and `item` is a `SDSFile` object describing the SDS file.

To include the new `exampleRule` in the execution, add a new object to the list
in the JSON rule map, mentioning the rule, and defining its options, like so:
```
{
    'name': 'exampleRule',
    'options': { ... }
}
```

Note that the `'options'` attribute is mandatory, even if empty.

## Implementing a new manager

To implement a new manager, operating on a new format of data, three
main steps are necessary:
1) Collecting data
2) Defining policies
3) Running the rule manager

### Collecting data

One option for computing a list of items to be operated is to extend
the `FileCollector` class.

This is a base class for the collection of files. The constructor
`FileCollector(path_to_dir)` returns a new `FileCollector` object with
the full filenames of all files inside the directory in a list
`self.files`.

It is possible to extend this class to develop more complex
collectors, or implement another collector entirely to use as a data
source for the manager. The only requirement is that the items are
passed in an iterable object.

### Defining policies

As mentioned above, policies are top-level functions in a module. For
a new manager, define all the desired rules in a new module, and pass
this module to the manager.

Then, define their order and options in a JSON file.

### RuleManager

The `RuleManager` is responsible for running the policies. A
`RuleManager` object takes a list of policies and applies them, in
sequence, to a list of objects.

An example on how to use it:

```python
rm = RuleManager() # Initialization, sets up logging
rm.loadRules(rules_module, rulemap_file) # Loads the rules
rm.sequence(item_list) # Executes the sequence of rules on all items
```

The `RuleManager` only applies the policies to the items. Both the
rules and the items are entirely transparent to it. It should run as
long as the functions defining the rules accept the data type passed.
