-module(proximity_lib_time).

%% API exports
-export([unix_to_gregorian/1]).
-export([unix_to_gregorian_datetime/1]).
-export([datetime_to_timestamp/1]).
-export([unix_timestamp/0, unix_timestamp/1]).
-export([unix_to_timestamp/1]).

%%====================================================================
%% API functions
%%====================================================================

unix_to_timestamp(UnixTimestamp) ->
    D = unix_to_gregorian_datetime(UnixTimestamp),
    datetime_to_timestamp(D).

unix_to_gregorian_datetime(UnixTimestamp) ->
    calendar:gregorian_seconds_to_datetime(unix_to_gregorian(UnixTimestamp)).

unix_to_gregorian(UnixTimestamp) ->
    UnixTimestamp + calendar:datetime_to_gregorian_seconds({{1970, 1, 1}, {0, 0, 0}}).

datetime_to_timestamp(DateTime) ->
    GSeconds = calendar:datetime_to_gregorian_seconds(DateTime),
    ESeconds = GSeconds - calendar:datetime_to_gregorian_seconds({{1970, 1, 1}, {0, 0, 0}}),
    {ESeconds div 1000000, ESeconds rem 1000000, 0}.

unix_timestamp() ->
    DateTime = calendar:universal_time(),
    calendar:datetime_to_gregorian_seconds(DateTime) - calendar:datetime_to_gregorian_seconds({{1970, 1, 1}, {0, 0, 0}}).

unix_timestamp({_Mega, _Sec, _Micro} = Now) ->
    DateTime = calendar:now_to_universal_time(Now),
    calendar:datetime_to_gregorian_seconds(DateTime) - calendar:datetime_to_gregorian_seconds({{1970, 1, 1}, {0, 0, 0}}).

%%====================================================================
%% Internal functions
%%====================================================================
