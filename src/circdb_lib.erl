
-module(circdb_lib).

-export([get_cfg/1,get_cfg/2,
	 current_time/0]).

-include("circdb_internal.hrl").

get_cfg(Cfg) ->
    emd_cfg:get_cfg_a(?APP_NAME,Cfg).

get_cfg(Cfg,Default) ->
    emd_cfg:get_cfg_a(?APP_NAME,Cfg,Default).


%%% Provides the current time in ms.
current_time() ->
    Now=erlang:timestamp(),
    S=calendar:datetime_to_gregorian_seconds(calendar:now_to_local_time(Now)),
    S*1000.
