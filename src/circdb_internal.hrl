-define(APP_NAME,circdb).


-record(db_table,{
	  size,       % (int) Size of database
	  delta,      % (int) Time (ms) between updates
	  trigger_fun,% (bool) Returns true if CurrPos in next table triggered
	  aggregator, % (min, max, average,last) Which value to store on trigger
	  first_time, % Time when curr_pos==0 is written
	  curr_pos=0, % The position in db written last time.
	  db          % (tuple) The actual data table. Only holds values
	 }).

