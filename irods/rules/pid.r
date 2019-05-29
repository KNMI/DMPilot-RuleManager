eudatPidSingleCheck2 {

  EUDATSearchPID(*path, *existing_pid)
  if (*existing_pid == "empty") {
    EUDATCreatePID(*parent_pid, *path, *ror, *fio, *fixed, *newPID);
    writeLine("stdout","PID-new: *newPID");
  }
  else {
    writeLine("stdout","PID-existing: *existing_pid");
  }
}
INPUT *path="null",*pid="null",*parent_pid="none",*ror="none",*fio="none",*fixed="False"
OUTPUT ruleExecOut
