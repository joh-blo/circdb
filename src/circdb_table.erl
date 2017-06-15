%%%-------------------------------------------------------------------
%%% @author Johan Blom <>
%%% @copyright (C) 2013, Johan Blom
%%% @doc
%%%
%%% @end
%%% Created : 13 Sep 2013 by Johan Blom <>
%%%-------------------------------------------------------------------
-module(circdb_table).

-behaviour(gen_server).

%% API
-export([start_link/2,
	 add_table/2,rm_table/3,
	 dump/1,
	 info/1,
	 update/3,
	 fetch/2,


	 get_data/2,
	 

	 fetch_range2/8
	]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-include("circdb_internal.hrl").


-define(SERVER, ?MODULE). 
-define(OTP_INTERNAL_GENCAST,'$gen_cast').
-define(YEAR1970SEC, 62167219200). % Gregorian seconds to 1970-01-01 00:00:00



%% Note that we assume only a single measurement for each process.
%% Thus, one process holds all time series for data associated with measurement
-record(state,{
	  name,      % Name of measurement 
	  cons_type, % Type of consolidation function
	  db,        % The actual data table
	  backup_interval
	 }).


%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Name,Input) ->
    gen_server:start_link(?MODULE, [Name,Input], []).

add_table(Pid,Tab) ->
    gen_server:call(Pid,{add_table,Tab}).
    
rm_table(Pid,Delta,Size) ->
    gen_server:call(Pid,{rm_table,Delta,Size}).



dump(Pid) ->
    gen_server:call(Pid,dump).

info(Pid) ->
    gen_server:call(Pid,info).

fetch(Pid,FetchArgs) ->
    gen_server:call(Pid,{fetch,FetchArgs}).



get_data(StartT,StopT) ->
    gen_server:call(?SERVER,{get_data,StartT,StopT}).

update(Pid,Time,V) ->
    gen_server:cast(Pid,{update,Time,V}).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([Name,Input]) ->
    %% Possibly restore data from some backup.
    Db=circdb_backup:restore(Name,Input),
    BI=circdb_lib:get_cfg(circdb_backup_interval),
    case circdb_lib:get_cfg(circdb_backup_start,undefined) of
	undefined ->
	    erlang:send_after(BI,self(),{?OTP_INTERNAL_GENCAST,backup});
	Time ->
	    %% Calculate time to next backup request
	    {CurrentDay,NextDay}=case calendar:local_time() of
				     {CD,CT} when CT<Time ->
					 {CD,0};
				     {CD,_} ->
					 {CD,1000*24*3600} % Next day
				 end,
	    BTime={CurrentDay,Time},
	    First=calendar:datetime_to_gregorian_seconds(BTime)*1000 -
		circdb_lib:current_time() +
		NextDay,
	    erlang:send_after(First,self(),{?OTP_INTERNAL_GENCAST,backup})
    end,
    {ok, #state{name=Name,
		db=Db,
		backup_interval=BI
	       }}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call({add_table,Tab}, _From, State=#state{db=Db}) ->
    io:format("JB-0 Adding Tab=~p~n",[Tab]),
    NewDb=add_to_db(Db,Tab,[]),
    {reply, ok, State#state{db=NewDb}};
handle_call({rm_table,Delta,Size}, _From, State=#state{db=Db}) ->
    NewDb=rm_from_db(Db,Delta,Size,[]),
    case NewDb of
	[] ->
	    {stop, normal, empty, State#state{db=NewDb}};
	_ ->
	    {reply, ok, State#state{db=NewDb}}
    end;
handle_call(dump, _From, State=#state{db=Db}) ->
    %% Dump all data entries with some additional info in some ASCII format
    Reply = [pp_dbt(DbT) || DbT <- Db],
    {reply, Reply, State};
handle_call(info, _From, State=#state{db=Db}) ->
    Reply = info_db(Db),
    {reply, Reply, State};
handle_call({fetch,FetchArgs}, _From, State=#state{db=Db}) ->
%    io:format("JB FetchArgs=~p~n Db=~p~n",[FetchArgs,Db]),
    Reply = read_db(FetchArgs,Db),
    {reply, Reply, State};
handle_call({get_data,StartT,StopT}, _From, State=#state{db=Db}) ->
    Reply = read_db({StartT,StopT},Db),
    {reply, Reply, State}.



%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------

handle_cast(backup,State=#state{name=Name,db=Db,backup_interval=BT}) ->
  
  circdb_backup:store(Name,Db),
  erlang:send_after(BT,self(),{?OTP_INTERNAL_GENCAST,backup}),
  {noreply, State};
handle_cast({update,Time,Y},State=#state{db=Db,cons_type=CT}) ->
%    io:format("~p update with ~p Db=~p~n",[?MODULE,Y,Db]),
    NewDb=update_db(Db,Time,Y,CT,[]),
    {noreply, State#state{db=NewDb}};
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(Info, State) ->
    io:format("Info=~p~n",[Info]),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% The table list (Db) is ordered such that the table with smallest time
%% between buckets is first. When adding we then iterate over this list until
%% we find a table that is larger (or the list is empty)
%% TODO:
%% - Validate config values. Want proper time series with no overlap for now
%% - Assumes tables are added in order. Assume normally tables are added by
%%   reading some configuration, so this should be ok for now...
%% - Must set trigger_fun by calculating Size*Delta and check all other existing
%%   table to see if any table should be written at that time
%% Note:
%% - Validates that the same table is not added twice.
add_to_db([],H0=#cdb_table{size=Size,delta=Delta},Out) ->
    NewH=H0#cdb_table{curr_pos=0,
		      db=list_to_tuple(lists:duplicate(Size,undefined))
		     },
    io:format("JB-0 Adding NewH=~p~n",[NewH]),
    %% FIXME! Must update trigger_fun in previous one...
    %% 
   NewOut=case Out of
	       [OH=#cdb_table{% size=OldSize,
		      delta=OldDelta}|NO] ->
		   TFV1=(Delta div OldDelta),
		   OldTriggFun=fun(V) ->
				       (V rem TFV1)==0
			       end,
		   OldH=OH#cdb_table{trigger_fun=OldTriggFun},
    io:format("OldDelta=~p Delta=~p~n",[OldDelta,Delta]),
		   [OldH|NO];
	       [] ->
		   []
	   end,
    lists:reverse([NewH|NewOut]);
add_to_db([H=#cdb_table{delta=ThisDelta}|Rest],
	  H0=#cdb_table{size=Size,delta=Delta},
	  Out)
  when ThisDelta>Delta ->
    %% This table has a *larger* time between buckets, so add new 
    %% OK, so add new (NewH) table before this (H) one and let NewH trigger H
    %% when at every pos (ThisDelta div Delta) in NewH
    NewOut=case Out of
	       [OH=#cdb_table{% size=OldSize,
			     delta=OldDelta}|NO] ->
		   TFV1=(OldDelta div ThisDelta),
		   OldTriggFun=fun(V) ->
				       (V rem TFV1)==0
			       end,
		   OldH=OH#cdb_table{trigger_fun=OldTriggFun},
    io:format("OldDelta=~p ThisDelta=~p Delta=~p~n",[OldDelta,ThisDelta,Delta]),
		   [OldH|NO];
	       [] ->
		   []
	   end,
    TFV2=(ThisDelta div Delta),
    TriggFun=fun(V) ->
		     (V rem TFV2)==0
	     end,
    NewH=H0#cdb_table{curr_pos=0,
		      trigger_fun=TriggFun,
		      db=list_to_tuple(lists:duplicate(Size,undefined))
		     },
    io:format("JB-1 Added NewH=~p~n",[NewH]),

    lists:reverse([H,NewH|NewOut])++Rest;
add_to_db([H=#cdb_table{delta=Delta}|Rest],
	  #cdb_table{size=_Size,delta=Delta},
	  Out) ->
  %% Trying to add a table with the same time between buckets as is already
  %% allocated - ignore this addition.
  lists:reverse(Out)++[H|Rest];
add_to_db([H|Rest],H0,Out) ->
    add_to_db(Rest,H0,[H|Out]).



rm_from_db([],_Delta,_Size,Out) ->
    lists:reverse(Out);
rm_from_db([#cdb_table{delta=Delta}|Rest],Delta,Size,Out) ->
    %% Remove this one. Now we must also adjust trigger_fun in table before and
    %% after.
    rm_from_db(Rest,Delta,Size,Out);
rm_from_db([H|Rest],Delta,Size,Out) ->
    rm_from_db(Rest,Delta,Size,[H|Out]).


info_db(Db) ->
    info_db_all(Db,[]).

info_db_all([],Out) ->
    lists:reverse(Out);
info_db_all([#cdb_table{first_time=FT,
			size=Size,
			delta=Delta,
			curr_pos=CurrPos}|Rest],
	    Out) ->
  Time=if
	 FT==undefined ->
	   FT;
	 true ->
	   DT0=calendar:now_to_datetime(FT),
	   GS0=calendar:datetime_to_gregorian_seconds(DT0),
	   GS=GS0+(Delta div 1000)*CurrPos,
	   DT1=calendar:now_to_datetime(erlang:timestamp()),
	   GS1=calendar:datetime_to_gregorian_seconds(DT1),	   
	   GS-GS1
       end,

    H=io_lib:format("~n  Size=~p Delta=~p CurrPos=~p"
		    " Next trigger in ~p seconds",
		    [Size,Delta,CurrPos,Time]),
    info_db_all(Rest,[H|Out]).



%% Currenty returns a list with tuples where the key is the time in seconds
%% However, should only return the values an nothing else here!
%% Formatting of data, including creation of tuples should rather be handled
%% in dc_handler when creatig graphs etc
read_db(last,[#cdb_table{% first_time=FT,
			curr_pos=CurrPos,
			% size=Size,
			db=DbD}|_]) ->
    if
	CurrPos==0 ->
	    undefined;
	true ->
%	    Time=1, % calendar:now_to_datetime(erlang:timestamp()),
	    %% Latest (most recently) stored data
%	    [{1,element(CurrPos,DbD)}]
	    element(CurrPos,DbD)
    end;
read_db(first,Db) ->
    %% Oldest stored data
    case lists:last(Db) of
	#cdb_table{curr_pos=0} ->
	    undefined;
	#cdb_table{curr_pos=CurrPos,
		  size=Size,
		  db=DbD} ->
	    FirstPos=(CurrPos rem Size)+1,
	    case element(FirstPos,DbD) of
		undefined ->
		    %% We have not yet stored any value here, search "forward"
		    %% by increasing CurrPos
		    %% FIXME: Isn't this always the first value ?
%		    [{1,search_fwd(CurrPos+1,CurrPos,Size,DbD)}];
		    search_fwd(CurrPos+1,CurrPos,Size,DbD);
		V ->
%		    [{1,V}]
		    V
	    end
    end;
read_db(all,[#cdb_table{first_time=FT,
		       curr_pos=CurrPos,size=Size,delta=Delta,db=Db}|_]) ->
    %% Return all data entries between StartT - StopT where StartT is the oldest
    %% stored value and StopT is the newest, i.e. all values in data base.
    %% FIXME! Currently just from one (the latest) table! Should do the same and
    %%     merge with older data (from other tables) as well.
    FirstPos=case (CurrPos+1) rem Size of
	       0 -> Size;
	       FP -> FP
	     end,
    TimeStart=calc_timestart(FT),
    fetch_range2(FirstPos,CurrPos,Size,1,TimeStart,Delta,Db,[]);
read_db({StartT,StopT},Db) ->
    %% Return all data entries between StartT - StopT
    %% Thus we have to calculate:
    %% - Which table to read from. To simplify we could say that all values
    %%   must be picked from the same table.
    DbT=find_dbt(Db,StartT,StopT),
    io:format("DbT=~p~n",[DbT]),

    %% - Which position is StartT in the selected table
    %% - Which position is StopT in the selected table
    io:format("StartT=~p~n"
	      "StopT =~p~n",[StartT,StopT]),
   fetch_range(DbT,StartT,StopT);
read_db(DiffT,Db) when is_integer(DiffT) ->
    %% Return all data entries no older than DiffT,
    %% ie between (erlang:timestamp()-DiffT) - erlang:timestamp() where DiffT is in ms
    io:format("DiffT=~p~n",[DiffT]),

    StopT=calendar:now_to_local_time(erlang:timestamp()),
    StartTms=calendar:datetime_to_gregorian_seconds(StopT)*1000-DiffT,
    StartT=calendar:gregorian_seconds_to_datetime(trunc(StartTms/1000)),
    DbT=find_dbt(Db,StartT,StopT),

    %% - Which position is StartT in the selected table
    %% - Which position is StopT in the selected table
    io:format("StartT=~p~n"
	      "StopT =~p~n",[StartT,StopT]),
    fetch_range(DbT,StartT,StopT);
read_db(_DiffT,_Db) ->
    {error,bad_cmd}.


fetch_range(#cdb_table{curr_pos=0},_StartT,_StopT) ->
    [];
fetch_range(#cdb_table{first_time=FT,
		      curr_pos=CurrPos,
		      size=Size,
		      delta=Delta,
		      db=Db},
	    RStartT,
	    RStopT) ->
    %% FT holds details on the time down to ms.
    FirstT=calendar:now_to_local_time(FT),

    NextPos=(CurrPos rem Size)+1,
    io:format("First =~p~n",[FirstT]),

    io:format("fetch_range CurrPos=~p~n NextPos=~p~n",[CurrPos,NextPos]),
    
    {FirstPos,LastPos}=calc_table_pos(CurrPos,Size,Delta,Db,FirstT,RStartT,RStopT),
    io:format("FirstPos=~p LastPos=~p~n",[FirstPos,LastPos]),
    TimeStart=calc_timestart(FT),
    fetch_range2(FirstPos,LastPos,Size,1,TimeStart,Delta,Db,[]).




calc_table_pos(CurrPos,Size,Delta,Db,FirstT,RStartT,RStopT) ->
  NextPos=(CurrPos rem Size)+1,
  FirstTms=calendar:datetime_to_gregorian_seconds(FirstT)*1000,
  RStartTms=calendar:datetime_to_gregorian_seconds(RStartT)*1000,
  RStopTms=calendar:datetime_to_gregorian_seconds(RStopT)*1000,
  case element(NextPos,Db) of
    undefined ->
      %% We have not yet filled up db table first time
      FP=case ((RStartTms-FirstTms) div Delta)+1 of
	   FP0 when FP0=<0 -> 1;
	   FP0 -> FP0
	 end,
      LP=case ((RStopTms-RStartTms) div Delta)+1 of
	   PosDiff when (PosDiff+FP)>CurrPos ->
	     CurrPos;
	   PosDiff ->
	     PosDiff+FP
	 end,
      {FP,LP};
    _ ->
      %% This is the lastest value stored, we have Size-1 older values
      CurrTms=FirstTms+(CurrPos-1)*Delta,
      io:format("Curr  = ~p delta=~p~n",
		[calendar:gregorian_seconds_to_datetime(trunc(CurrTms/1000)),
		 (CurrPos-1)*Delta]),
      
      if
	RStartTms>CurrTms ->
	  {0,0};
	RStopTms<(CurrTms-Size*Delta) ->
	  {0,0};
	true ->
	  %% Distance from CurrPos, backwards
	  FP=calc_pos(CurrTms,RStartTms,Delta,CurrPos,Size),
	  LP=calc_pos(CurrTms,RStopTms,Delta,CurrPos,Size),
	  {FP,LP}
      end
  end.



calc_pos(T1,T2,Delta,CurrPos,Size) ->
    TDiff=case T1-T2 of
	      TD0 when TD0<0 -> 0;
	      TD0 -> TD0
	  end,
    Tplus=case TDiff rem Delta of
	      0 -> 0;
	      _ -> 1
	  end,
    P1=(TDiff div Delta)+Tplus,
%    io:format("calc_pos CurrPos=~p PT=~p Tplus=~p~n",[CurrPos,(TDiff div Delta),Tplus]),
    case (CurrPos-P1) rem Size of
	P2 when P2=<0 -> P2+Size;
	P2 -> P2
    end.

%% Calculates number of ms from 1970-01-01
calc_timestart(undefined) ->
  0;
calc_timestart(FT) ->
  NowT=calendar:datetime_to_gregorian_seconds(calendar:now_to_local_time(FT)),
  1000*(NowT - ?YEAR1970SEC).



fetch_range2(_,0,_Size,_Cnt,_TimeStart,_DeltaSec,_Db,_Out) ->
    %% Detected out of range
    [];
fetch_range2(Pos,Pos,_Size,Cnt,TimeStart,DeltaSec,Db,Out) ->
    V=element(Pos,Db),
    Time=TimeStart+Cnt*DeltaSec,
    NewOut=if
	     V==undefined -> Out;
	     true -> [{Time,V}|Out]
	   end,
    lists:reverse(NewOut);
fetch_range2(CPos,LPos,Size,Cnt,TimeStart,DeltaSec,Db,Out) ->
    V=element(CPos,Db),
    NewCPos=(CPos rem Size)+1,
    Time=TimeStart+Cnt*DeltaSec,
    NewOut=if
	     V==undefined -> Out;
	     true -> [{Time,V}|Out]
	   end,
    fetch_range2(NewCPos,LPos,Size,Cnt+1,TimeStart,DeltaSec,Db,NewOut).

    

find_dbt([H],_StartT,_StopT) ->
    H;
find_dbt([#cdb_table{curr_pos=0}|Rest],StartT,StopT) ->
    find_dbt(Rest,StartT,StopT);
find_dbt([DbT=#cdb_table{first_time=FT,
			curr_pos=CurrPos,
			size=Size,
			delta=Delta,
			db=Db}|Rest],StartT,StopT) ->
    %% Assumption: StartT and StopT on datetime format

    FirstT=calendar:now_to_local_time(FT),
    FirstTms=calendar:datetime_to_gregorian_seconds(FirstT)*1000,
    {OldestT,LatestT}=
	case element(CurrPos,Db) of
	    undefined ->
		%% We have not yet filled up db table first time
		LastTs=trunc((FirstTms+Size*Delta)/1000),
		LastT=calendar:gregorian_seconds_to_datetime(LastTs),
		{FirstT,LastT};
	    _ ->
		%% Oldest is then at CurrPos
		OldTs=trunc((FirstTms-(Size-CurrPos)*Delta)/1000),
		OldT=calendar:gregorian_seconds_to_datetime(OldTs),
		LastTs=trunc((OldTs*1000+Size*Delta)/1000),
		LastT=calendar:gregorian_seconds_to_datetime(LastTs),
		{OldT,LastT}
	end,
    if
	StartT>=OldestT,
	LatestT>=StopT ->
	    DbT;
	true ->
	    find_dbt(Rest,StartT,StopT)
    end.
	

search_fwd(Pos,Pos,_Size,_Db) ->
    undefined;
search_fwd(Pos,StartPos,Size,Db) ->
    CheckPos=(((Pos-1) rem Size)+1),
    case element(CheckPos,Db) of
	undefined ->
	    search_fwd(CheckPos+1,StartPos,Size,Db);
	V ->
	    V
    end.



pp_dbt(#cdb_table{curr_pos=0,
		 size=Size}) ->
    io_lib:format("Not in use~nSize =~p~n",[Size]);
pp_dbt(#cdb_table{first_time=FT,
		 size=Size,
		 delta=Delta,
		 curr_pos=CurrPos,
		 trigger_fun=TriggFun,
		 db=Db}) ->
    F=calendar:now_to_local_time(FT),
    LastTms=calendar:datetime_to_gregorian_seconds(F)*1000+Size*Delta,
    L=calendar:gregorian_seconds_to_datetime(trunc(LastTms/1000)),
    TriggFlag=if
		  is_function(TriggFun) ->
		      TriggFun(CurrPos);
		  true ->
		      undefined
	      end,
    StrD=tuple_to_list(Db),
    io_lib:format("Start=~p~n"
		  "Stop= ~p~n"
		  "Size =~p~n"
		  "CurrP=~p~n"
		  "TrigF=~p~n"
		  "Db   =~p~n",
		  [F,L,Size,CurrPos,TriggFlag,StrD]).



%% Update first table in list with new value.
%% If we reach a trigger position, this update trigger one or more additional 
%% updates in the table following.
%% Note:
%% - We cannot trust the data provider to provide new values at the right time!
update_db([],_Time,_Y,_CT,Out)  ->
    lists:reverse(Out);
update_db([H=#cdb_table{curr_pos=CurrPos,
			trigger_fun=TriggFun,
			size=Size,
			db=Db} | Rest],
	  Time,Y,CT,Out)  ->
    NewPos=(((CurrPos-1+1) rem Size)+1),
    NewH=if
	     NewPos==1 ->
		 H#cdb_table{first_time=erlang:timestamp(),
			     curr_pos=NewPos,
			     db=setelement(NewPos,Db,Y)};
	     true ->
		 H#cdb_table{curr_pos=NewPos,
			     db=setelement(NewPos,Db,Y)}
	 end,
    TriggerNext=if
		    is_function(TriggFun) ->
			TriggFun(CurrPos);
		    true ->
			false
		end,
%    io:format("Step=~p CurrPos=~p TriggerNext=~p~n",[Step,CurrPos,TriggerNext]),
    if
	TriggerNext ->
	    update_db(Rest,Time,Y,CT,[NewH|Out]);
	true ->
	    lists:reverse([NewH|Out])++Rest
    end.
