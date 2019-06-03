Replication {
    *registered = "true";
    *recursive = "true";
    *status = EUDATReplication(*source, *destination, *registered, *recursive, *response);
    if (*status) {
        writeLine("stdout", "Replica *source on *destination Success!");
    }
    else {
        writeLine("stdout", "Replica *source on *destination Failed: *response");
    }
}
