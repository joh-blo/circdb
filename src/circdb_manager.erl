%%% @author Johan Blom <>
%%% @copyright (C) 2013, Johan Blom
%%% @doc
%%%
%%% @end
%%% Created : 12 Sep 2013 by Johan Blom <>

-module(circdb_manager).
-behaviour(gen_server).


-export([create/2,
	 
%	 new/2,
	 
	 delete/3,
	 update/2,update/3,
	 updatev/1, dump/1, restore/1, last/1,
         first/1,
	 info/0, info/1,
	 fetch/2, tune/1, resize/1, xport/1,
         lastupdate/1
         ]).

%% Gen server exports
-export([start_link/0,
	 init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-include("circdb_internal.hrl").


-record(state,{
	  table_db,      % (ets) Table Db, with preconfigured RRD definitions
	  measurement_db % (proplist) Mappings with measurement name and Pid
	 }).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Public 
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


%% @spec start_link() -> Result
%%   Result = {ok,Pid} | ignore | {error,Error}
%%     Pid = pid()
%%     Error = {already_started,Pid} | shutdown | term()
%% @doc cClls gen_server:start_link
start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE,[], []).



%new(Name,Input) ->
%    gen_server:call(?MODULE,{new,Name,Input}).


%% @spec create(erlang:iodata()) -> { ok, Response }  |  
%%   { error, Reason } 
%%  Reason = iolist()
%%  Response = iolist()
%% @doc Create a new Round Robin Database (RRD). Check 
%% [http://oss.oetiker.ch/rrdtool/doc/rrdcreate.en.html rrdcreate].
create(TabIds,Name) ->
    gen_server:call(?MODULE,{create,TabIds,Name}).

%% @doc Delete an existing Round Robin Database (RRD).
delete(Name,Step,Repeats) ->
    gen_server:call(?MODULE,{delete,Name,Step,Repeats}).


-spec update(Name::string(),V::erlang:iodata()) ->
		    {ok, Response::atom()}  |  {error, Reason::atom()}.
%%  Reason = iolist()
%%  Response = iolist()
%% @doc Store new data values into an RRD. Check 
%% [http://oss.oetiker.ch/rrdtool/doc/rrdupdate.en.html rrdupdate].
update(Name,V) ->
    Time=circdb_lib:current_time(),
    gen_server:cast(?MODULE,{update,Name,Time,V}).

update(Name,Time,V) ->
    gen_server:cast(?MODULE,{update,Name,Time,V}).


%% @spec updatev(erlang:iodata()) -> { ok, Response }  |  
%%   { error, Reason } ssh
%%  Reason = iolist()
%%  Response = iolist()
%% @doc Operationally equivalent to update except for output. Check 
%% [http://oss.oetiker.ch/rrdtool/doc/rrdupdate.en.html rrdupdate].
updatev(Args) ->
    gen_server:call(?MODULE,{updatev,Args}).

%% @spec dump(erlang:iodata()) -> { ok, Response }  |  
%%   { error, Reason } 
%%  Reason = iolist()
%%  Response = iolist()
%% @doc    Dump the contents of an RRD in plain ASCII. In connection with
%%         restore you can use this to move an RRD from one computer
%%         architecture to another.  Check 
%% [http://oss.oetiker.ch/rrdtool/doc/rrddump.en.html rrddump].
dump(Name) ->
    gen_server:call(?MODULE,{dump,Name}).

%% @spec restore(erlang:iodata()) -> { ok, Response }  |  
%%   { error, Reason } 
%%  Reason = iolist()
%%  Response = iolist()
%% @doc Restore an RRD in XML format to a binary RRD. Check 
%% [http://oss.oetiker.ch/rrdtool/doc/rrdrestore.en.html rrdrestore]
restore(Args) ->
    gen_server:call(?MODULE,{restore,Args}).

%% @spec last(erlang:iodata()) -> { ok, Response }  |  
%%   { error, Reason } 
%%  Reason = iolist()
%%  Response = integer()
%% @doc    Return the date of the last data sample in an RRD. Check 
%% [http://oss.oetiker.ch/rrdtool/doc/rrdlast.en.html rrdlast]
last(Name) -> 
    gen_server:call(?MODULE,{last,Name}).

%% @spec lastupdate(erlang:iodata()) -> { ok, Response }  |  
%%   { error, Reason } 
%%  Reason = iolist()
%%  Response = iolist()
%% @doc    Return the most recent update to an RRD. Check 
%% [http://oss.oetiker.ch/rrdtool/doc/rrdlastupdate.en.html rrdlastupdate]
lastupdate(Args) ->
    gen_server:call(?MODULE,{lastupdate,Args}).

%% @spec first(erlang:iodata()) -> { ok, Response }  |  
%%   { error, Reason } 
%%  Reason = iolist()
%%  Response = integer()
%% @doc Return the date of the first data sample in an RRA within an
%%       RRD. Check 
%% [http://oss.oetiker.ch/rrdtool/doc/rrdfirst.en.html rrdfirst]
first(Name) ->
    gen_server:call(?MODULE,{first,Name}).


%% @spec info(erlang:iodata()) -> { ok, Response }  |  
%%   { error, Reason } 
%%  Reason = iolist()
%%  Response = iolist()
%% @doc Get information about an RRD. Check 
%% [http://oss.oetiker.ch/rrdtool/doc/rrdinfo.en.html rrdinfo].
info() ->
    gen_server:call(?MODULE,{info,all}).

info(Args) ->
    gen_server:call(?MODULE,{info,Args}).

%% @spec fetch(erlang:iodata()) -> { ok, Response }  |  
%%   { error, Reason } 
%%  Reason = iolist()
%%  Response = iolist()
%% @doc   Get data for a certain time period from a RRD. The graph func-
%%         tion uses fetch to retrieve its data from an RRD. Check 
%% [http://oss.oetiker.ch/rrdtool/doc/rrdfetch.en.html rrdfetch].
fetch(Name,FetchArgs) ->
    gen_server:call(?MODULE,{fetch,Name,FetchArgs}).

%% @spec tune(erlang:iodata()) -> { ok, Response }  |  
%%   { error, Reason } 
%%  Reason = iolist()
%%  Response = iolist()
%% @doc Alter setup of an RRD. Check 
%% [http://oss.oetiker.ch/rrdtool/doc/rrdtune.en.html rrdtune].
tune(Args) ->
    gen_server:call(?MODULE,{tune,Args}).

%% @spec resize(erlang:iodata()) -> { ok, Response }  |  
%%   { error, Reason } 
%%  Reason = iolist()
%%  Response = iolist()
%% @doc    Change the size of individual RRAs. This is dangerous! Check
%%         rrdresize.
resize(Args) ->
    gen_server:call(?MODULE,{resize,Args}).

%% @spec xport(erlang:iodata()) -> { ok, Response }  |  
%%   { error, Reason } 
%%  Reason = iolist()
%%  Response = iolist()
%% @doc   Export data retrieved from one or several RRDs. Check 
%% [http://oss.oetiker.ch/rrdtool/doc/rrdxport.en.html rrdxport]
%%
%%  erlrrd:xport("'DEF:foo=/path with/space/foo.rrd:foo:AVERAGE' XPORT:foo").
%%
%%  erlrrd:xport(erlrrd:c(["DEF:foo=/path with/space/foo.rrd:foo:AVERAGE", "XPORT:foo"])).
xport(Args) ->
    gen_server:call(?MODULE,{xport,Args}).



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Gen server interface poo
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @hidden
init([]) -> 
    TDB=init_tdb(),
    {ok, #state{table_db=TDB,
		measurement_db=[]}}.

%% terminate
%% @hidden
terminate(_Reason, _State) -> ok.


%%
%% handle_call
%% @hidden
handle_call({create,TabIds,Name}, _From,State=#state{table_db=TDB,
						     measurement_db=MDB}) ->
    io:format("create Tables=~p~n",[tdb_to_list(TDB)]),
    {Resp,NewMDB}=alloc_measurement(TabIds,Name,TDB,MDB),
    {reply, Resp, State#state{measurement_db=NewMDB}};


%% handle_call({new,Name,Input}, _From,State=#state{table_db=TDB}) ->
%%     {Resp,NewTDB}=new_table(Name,Input,TDB),
%%     {reply, Resp, State#state{table_db=NewTDB}};
handle_call({delete,Name,Step,Repeats}, _From,State=#state{measurement_db=MDB}) ->
    {Resp,NewMDB}=delete_table(Name,Step,Repeats,MDB),
    {reply, Resp, State#state{measurement_db=NewMDB}};
handle_call({last,Name}, _From,State=#state{measurement_db=MDB}) ->
    Resp=lookup_table(Name,last,MDB),
    {reply, Resp, State};    
handle_call({first,Name}, _From,State=#state{measurement_db=MDB}) ->
    Resp=lookup_table(Name,first,MDB),
    {reply, Resp, State};    
handle_call({fetch,Name,FetchArgs}, _From,State=#state{measurement_db=MDB}) ->
    Resp=lookup_table(Name,FetchArgs,MDB),
    {reply, Resp, State};    
handle_call({dump,Name}, _From,State=#state{measurement_db=MDB}) ->
    Resp=case dump_table(Name,MDB) of
	     Str when is_list(Str) ->
		 io:format("~s~n",[Str]),
		 ok;
	     Err ->
		 Err
	 end,
    {reply, Resp, State};
handle_call({info,Arg}, _From,State=#state{measurement_db=MDB}) ->
    Resp=case info_table(MDB,Arg) of
	     Str when is_list(Str) ->
		 io:format("~s~n",[Str]),
		 ok;
	     Err ->
		 Err
	 end,
    {reply, Resp, State}.


%% handle_cast
%% @hidden
handle_cast({update,Name,Time,V}, State=#state{measurement_db=MDB}) ->
    update_table(Name,Time,V,MDB),
    {noreply, State};    
handle_cast(Msg, State) -> 
    io:format("Got unexpected cast msg: ~p~n", [Msg]),
    {noreply, State}.

%% handle_info
%% @hidden
handle_info(Msg, State) -> 
    io:format("Got unexpected info msg: ~p~n", [Msg]),
    {noreply, State}.


%% @hidden
code_change(_OldVsn, State, _Extra) -> 
  {ok, State}.


%%% ----------------------------------------------------------------------------




%% new_table(Name,Input,TDB) ->
%%   case proplists:get_value(Name,TDB) of
%%     undefined ->
%%       case circdb_table:start_link(Name,Input) of
%% 	{ok,Pid} ->
%% 	  {{ok,Pid},[{Name,Pid}|TDB]};
%% 	Error ->
%% 	io:format("ERROR ~p:new_table ~p got ~p~n",
%% 		  [?MODULE,Name,Error]),
%% 	  {Error,TDB}
%%       end
%%   end.





alloc_measurement(TabIds,Name,TDB,MDB) ->
    Input=undefined,
    case circdb_table:start_link(Name,Input) of
	{ok,Pid} ->
	    alloc_table2(TabIds,Pid,TDB),
	    case proplists:get_value(Name,MDB) of
		undefined ->
		    {ok,[{Name,Pid}|MDB]};  
		_ ->
		    {ok,lists:keyreplace(Name,1,MDB,{Name,Pid})}
	    end;
	Error ->
	    io:format("ERROR ~p:alloc_table ~p got ~p~n",
		      [?MODULE,Name,Error]),
	    {Error,MDB}
    end.

alloc_table2([],_Pid,_TDB) ->
    ok;
alloc_table2([TabId|Rest],Pid,TDB) ->
    case ets:lookup(TDB,TabId) of
      [] ->
	    io:format("ERROR ~p:alloc_table2 ~p unknown table id~n",
		      [?MODULE,TabId]),
	    {error,table_not_initialized};
      [Tab] ->
	    circdb_table:add_table(Pid,Tab),
	    alloc_table2(Rest,Pid,TDB)
    end.

delete_table(Name,Step,Repeats,MDB) ->
    case proplists:get_value(Name,MDB) of
	undefined ->
	    {{error,unknown_measurement},MDB};
	Pid ->
	    case circdb_table:rm_table(Pid,Step,Repeats) of
		empty ->
		    {ok,lists:keydelete(Name,1,MDB)};
		_ ->
		    {ok,MDB}
	    end
    end.

update_table(Name,Time,V,MDB) ->
    case proplists:get_value(Name,MDB) of
	undefined ->
	io:format("ERROR Unknown measurement ~p~n",[Name]),
	    {error,unknown_measurement};
	Pid ->
	    circdb_table:update(Pid,Time,V)
    end.
    

lookup_table(Name,FetchArgs,MDB) ->
    case proplists:get_value(Name,MDB) of
	undefined ->
	    {error,unknown_measurement};
	Pid ->
	    circdb_table:fetch(Pid,FetchArgs)
    end.
 


dump_table(Name,MDB) ->
    case proplists:get_value(Name,MDB) of
	undefined ->
	    {error,unknown_measurement};
	Pid ->
	    circdb_table:dump(Pid)
    end.


info_table(MDB,all) ->
    info_table_all(MDB,[]);
info_table(MDB,MeasurementId) ->
  case proplists:get_value(MeasurementId,MDB) of
    Pid when is_pid(Pid) ->
      io_lib:format("Name=~p"
		    " Info=~s~n",
		    [MeasurementId,lists:flatten(circdb_table:info(Pid))]);
    Error ->
      io:format("ERROR ~p:info_table ~p~n",[?MODULE,Error]),
      {error,unknown_table}
  end.


info_table_all([],Out) ->
    lists:reverse(Out);
info_table_all([{Name,Pid}|Rest],Out) ->
    H=lists:flatten(io_lib:format("Name=~p"
				  " Info=~s~n",
				  [Name,lists:flatten(circdb_table:info(Pid))])),
    info_table_all(Rest,[H|Out]).


%%% ----------------------------------------------------------------------------
%% DB primitives
init_tdb() ->
    TS0=circdb_lib:get_cfg(circdb_tables,[]),
    Db=ets:new(tsdb,[{keypos,#cdb_table.id}]),
    TS=init_tables(TS0,[]),
    ets:insert(Db,TS),
    io:format("Db=~p~n TS=~p~n",[Db,TS]),
    Db.


init_tables([],Out) ->
    lists:reverse(Out);
init_tables([H=#cdb_table{id=TabId,delta=Interval,size=Buckets}|Rest],
	    Out) ->
    io:format("init_tables ~p Interval=~p Buckets=~p~n",
	      [TabId,Interval,Buckets]),
    init_tables(Rest,[H|Out]).



tdb_to_list(TDB) ->
    ets:tab2list(TDB).
