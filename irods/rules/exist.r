FederatedExistence {
    *status = EUDATObjExist(*path, *response);
    if (*status) {
        writeLine("stdout", "True: *response");
    }
    else {
        writeLine("stdout", "False: *response");
    }
}
