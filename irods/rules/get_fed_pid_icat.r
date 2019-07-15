eudatiPidSearch {

  *FVALUE = "None";
  EUDATgetLastAVU(*path, "PID", *FVALUE);

  if(*FVALUE == "None") {
    writeLine("stdout","Failure: None");
  } else {
    writeLine("stdout","Success: *FVALUE");
  }

}
