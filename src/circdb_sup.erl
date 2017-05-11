%%% @author Johan <>
%%% @copyright (C) 2017, Johan
%%% @doc
%%%
%%% @end
%%% Created : 11 May 2017 by Johan <>

-module(circdb_sup).

-behaviour(supervisor).

%% public
-export([start_link/1,init/1]).



%%%----------------------------------------------------------------------
%%% API
%%%----------------------------------------------------------------------
start_link(AppList) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, AppList).



%%%----------------------------------------------------------------------
%%% Callback functions from supervisor
%%%----------------------------------------------------------------------

%%----------------------------------------------------------------------
%% Func: init/1
%% Returns: {ok,  {SupFlags,  [ChildSpec]}} |
%%          ignore                          |
%%          {error, Reason}   
%%----------------------------------------------------------------------
init(_AppList) ->
    %% Create a supervisor for each application in AppList
    BackupDir="/tmp",
    Specs=[{circdb_manager,
	    {circdb_manager,start_link,[]},
	    transient,2000,worker,[circdb]},
	   {circdb_backup,
	    {circdb_backup,start_link,[BackupDir]},
	    transient,2000,worker,[circdb]}
	  ],

    {ok,{{one_for_all,10,3},Specs}}.
