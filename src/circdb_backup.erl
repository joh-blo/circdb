%%%-------------------------------------------------------------------
%%% @author johan <>
%%% @copyright (C) 2016, johan
%%% @doc
%%%
%%% @end
%%% Created : 30 May 2016 by johan <>
%%%-------------------------------------------------------------------
-module(circdb_backup).

-behaviour(gen_server).

%% API
-export([start_link/1,
	 store/2,restore/2,

	 prefetch/1
	]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-include("circdb_internal.hrl").

-define(SERVER, ?MODULE).

-record(state,{
	  db  % Pointer to DETS data base to store all table contents in
	 }).

%%%===================================================================
%%% API
%%%===================================================================
store(Name,Data) ->
    gen_server:call(?MODULE,{store,Name,Data}).

restore(Name,Input) ->
    gen_server:call(?MODULE,{restore,Name,Input}).
  

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Dir) ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], [Dir]).

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
init(Dir) ->
  Name=backup,
  {ok, #state{db=open_dets(Name,Dir)}}.

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
handle_call({store,Name,Data}, _From, State=#state{db=Db}) ->

%  io:format("Generate backup DS ~p! Data=~p~n",[Name,Data]),
  Reply=dets:insert(Db,{Name,Data}),

  {reply, Reply, State};
handle_call({restore,Name,Input}, _From, State=#state{db=Db}) ->
  
%  io:format("Restore backup DS ~p!~n",[Name]),
  Reply=case catch dets:lookup(Db,Name) of
	  [] ->
	    %% This is not yet stored, use input fuction to possibly prefetch
	    %% data
	    case prefetch(Input) of
	      undefined ->
		[];
	      Data ->
		dets:insert(Db,{Name,Data}),
		Data
	    end;
	  [{_,Resp}] when is_list(Resp) -> Resp;
	  Error ->
	    io:format("Error=~p~n",[Error]),
	    []
	end,
  {reply, Reply, State}.


prefetch({{node,Node},{M,F,A}}) ->
  case catch rpc:call(Node,M,F,A++[prefetch]) of
    {List,Size,Delta,FT,CurrPos} when is_list(List) ->
      %% Should create a list with #db_table{} from data 
      [#db_table{curr_pos=CurrPos,
		 size=Size,
		 delta=Delta,
		 first_time=FT,
		 db=list_to_tuple(List)
		}];
    _ ->
      undefined
  end.



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
handle_info(_Info, State) ->
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

open_dets(Name,Dir) when is_atom(Name) ->
  Opts=[ {access, read_write}
       , {file, filename:join([Dir,"db",atom_to_list(Name)++".dets"])}
       , {type, set}
  ],
  case dets:open_file(Name,Opts) of
    {ok,Name} ->
      Name;
    Error ->
      io:format("ERROR: Can't open database, got ~p~n",[Error])
  end.
