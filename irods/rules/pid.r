eudatPidSingleCheck2 {

  EUDATSearchPID(*path, *existing_pid)

  if(*existing_pid == "empty") {
    EUDATCreatePID(*parent_pid, *path, *ror, *fio, *fixed, *newPID);
    writeLine("stdout","PID-new: *newPID");
  } else {
    writeLine("stdout","PID-existing: *existing_pid");
  }

}
