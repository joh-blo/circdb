%%% @author Johan <>
%%% @copyright (C) 2017, Johan
%%% @doc
%%%
%%% @end
%%% Created : 12 Feb 2017 by Johan <>

-module(circdb).

-export([map_record/2]).

-include("circdb_internal.hrl").

%% --- Specific configuration records for circdb
map_record(RecordType,Fields) ->
    case RecordType of
	cdb_table ->
	    DecFields=record_info(fields,cdb_table),
	    Defaults=[],
	    FieldList=emd_cfg:create_record_list(DecFields,Fields,Defaults,[]),
	    list_to_tuple([RecordType|FieldList])
    end.
