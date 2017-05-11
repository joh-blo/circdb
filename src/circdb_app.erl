%%% _________________________________________________________________________
%%% Copyright (C) 2015 by Johan Blom
%%%
%%% _________________________________________________________________________
%%% Overview:
%%% <ul>
%%% <li> Input data is a single value </li>
%%% <li> A table (time serie) is a circularly ordered collection of buckets
%%%    holding data with a defined interval between each bucket, </li>
%%% <li> For a collection of input data, several tables may be used differing on
%%%    the interval between each bucket. </li>
%%% <li> Use one process for each such collection of tables </li>
%%% </ul>
%%% TODO: 
%%% - Add support for consolidation function, eg MAX, MIN, AVERAGE, LAST values
%%% - Add support for vector data
%%% - Use Mnesia as backend (easy), but cache all relevant data in memory
%%%     (speed)
%%%    + Store on disc only occasionally (configurable how often) </li>
%%%   
%%% _________________________________________________________________________


-module(circdb_app).
-revision('$Id: smpp_app.erl,v 1.11 2009/06/04 14:32:03 johan Exp $ ').
-author('johan.blom@mobilearts.se').
-modified('$Date: 2009/06/04 14:32:03 $ ').
-modified_by('').
-vsn('1').

-behaviour(application).

%% application callbacks
-export([start/2, stop/1]).

%% Testing: erl -pa ../ebin/

%%%----------------------------------------------------------------------
%%% Callback functions from application
%%%----------------------------------------------------------------------

%%----------------------------------------------------------------------
%% Func: start/2
%% Returns: {ok, Pid}        |
%%          {ok, Pid, State} |
%%          {error, Reason}   
%%----------------------------------------------------------------------
start(normal,[]) ->
    circdb_sup:start_link([]).

%%----------------------------------------------------------------------
%% Func: stop/1
%% Returns: any 
%%----------------------------------------------------------------------
stop(_State) ->
    ok.


%%%----------------------------------------------------------------------
%%% Internal functions
%%%----------------------------------------------------------------------
