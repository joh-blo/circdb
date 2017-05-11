
-module(circdb_lib).

-export([get_cfg/1,get_cfg/2]).

-include("circdb_internal.hrl").

get_cfg(Cfg) ->
    emd_cfg:get_cfg_a(?APP_NAME,Cfg).

get_cfg(Cfg,Default) ->
    emd_cfg:get_cfg_a(?APP_NAME,Cfg,Default).
