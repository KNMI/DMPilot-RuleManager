eudatPidSearch {

  EUDATSearchPID(*path, *existing_pid)

  if(*existing_pid == "empty") {
    writeLine("stdout","Failure: None");
  } else {
    writeLine("stdout","Success: *existing_pid");
  }

}
