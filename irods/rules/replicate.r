Replication {
    *registered = "true";
    *recursive = "true";
    *status = EUDATReplication(*source, *destination, *registered, *recursive, *response);
    if (*status) {
        writeLine("stdout", "Success: *source replicated on *destination");
    }
    else {
        writeLine("stdout", "Failed: *response");
    }
}
